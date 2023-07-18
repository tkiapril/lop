import { encode as base64urlencode } from "https://deno.land/std@0.194.0/encoding/base64url.ts";
import { parse as parseArgs } from "https://deno.land/std@0.194.0/flags/mod.ts";
import { Buffer } from "https://deno.land/std@0.194.0/io/mod.ts";

import { BasicProperties } from "https://deno.land/x/amqp@v0.23.1/mod.ts";
import {
  BASIC,
  BASIC_ACK,
  BASIC_CANCEL,
  BASIC_CANCEL_OK,
  BASIC_CONSUME,
  BASIC_CONSUME_OK,
  BASIC_DELIVER,
  BASIC_PUBLISH,
  CHANNEL,
  CHANNEL_CLOSE,
  CHANNEL_CLOSE_OK,
  CHANNEL_OPEN,
  CHANNEL_OPEN_OK,
  CONNECTION,
  CONNECTION_CLOSE,
  CONNECTION_CLOSE_OK,
  CONNECTION_OPEN,
  CONNECTION_OPEN_OK,
  CONNECTION_START,
  CONNECTION_START_OK,
  CONNECTION_TUNE,
  CONNECTION_TUNE_OK,
  EXCHANGE,
  EXCHANGE_DECLARE,
  EXCHANGE_DECLARE_OK,
  HARD_ERROR_NOT_ALLOWED,
  QUEUE,
  QUEUE_BIND,
  QUEUE_BIND_OK,
  QUEUE_DECLARE,
  QUEUE_DECLARE_OK,
  SOFT_ERROR_NOT_FOUND,
} from "https://deno.land/x/amqp@v0.23.1/src/amqp_constants.ts";
import { IncomingFrame } from "https://deno.land/x/amqp@v0.23.1/src/amqp_frame.ts";
import { AmqpSocket } from "https://deno.land/x/amqp@v0.23.1/src/amqp_socket.ts";
import { FrameError } from "https://deno.land/x/amqp@v0.23.1/src/frame_error.ts";

type Message = {
  exchange: string;
  routingKey: string;
  props: BasicProperties;
  data: Uint8Array;
};

function main() {
  const flags = parseArgs(Deno.args, {
    boolean: ["help"],
    string: ["port"],
  });
  const port = Number(flags.port);
  if (flags.help) {
    console.log("--port: port to listen.");
    return;
  }
  if (
    flags.port != undefined &&
    (!Number.isInteger(flags.port) || port < 0 || port > 65535)
  ) {
    throw new Deno.errors.AddrNotAvailable(`Invalid port '${flags.port}'.`);
  }
  broker({ port: Number.isInteger(port) ? port : undefined });
}

export function broker(
  options?: Omit<Deno.TcpListenOptions & { transport?: "tcp" }, "port"> & {
    port?: number;
  },
) {
  const exchanges: Record<
    string,
    { type: string; route: Record<string, string[]> }
  > = {};
  const queues: Record<
    string,
    {
      messages: Message[];
      consumers: {
        deliver: (message: Message) => Promise<void>;
        cancel: AbortSignal;
      }[];
      lastDelivered: number;
    }
  > = {};
  const listener = Deno.listen({
    ...options,
    port: options?.port != undefined ? options.port : 5672,
  });
  const abortBroker = new AbortController();
  const conns: Deno.Conn[] = [];
  abortBroker.signal.onabort = () => {
    const errors = conns.reduce((acc, conn) => {
      try {
        conn.close();
      } catch (e) {
        if (!(e instanceof Deno.errors.BadResource)) return [...acc, e];
      }
      return acc;
    }, [] as Error[]);
    try {
      listener.close();
    } catch (e) {
      if (!(e instanceof Deno.errors.BadResource)) errors.push(e);
    }
    if (errors.length > 0) {
      throw errors.length > 1 ? new AggregateError(errors) : errors[0];
    }
  };

  (async () => {
    for await (const conn of listener) {
      if (abortBroker.signal.aborted) {
        const errors = [conn, listener].reduce((acc, conn) => {
          try {
            conn.close();
          } catch (e) {
            if (!(e instanceof Deno.errors.BadResource)) return [...acc, e];
          }
          return acc;
        }, [] as Error[]);
        if (errors.length > 0) {
          throw errors.length > 1 ? new AggregateError(errors) : errors[0];
        }
        return;
      }
      conns.push(conn);
      handleConn(conn);
    }
  })();

  return () => abortBroker.abort();

  async function handleConn(conn: Deno.Conn) {
    const magicBuf = new Uint8Array(8);
    const magicBytesRead = await conn.read(magicBuf);
    if (
      magicBytesRead !== 8 ||
      !uint8ArrayEqual(magicBuf, new Uint8Array([65, 77, 81, 80, 0, 0, 9, 1]))
    ) {
      conn.close();
      return;
    }

    const socket = new AmqpSocket(conn);
    const abortConn = new AbortController();
    const consumerTags: Record<number, Record<string, AbortController>> = {};

    try {
      await socket.write([{
        channel: 0,
        type: "method",
        payload: {
          classId: CONNECTION,
          methodId: CONNECTION_START,
          args: {
            serverProperties: {},
          },
        },
      }]);

      while (!abortConn.signal.aborted) {
        await handleFrame(await socket.read(), abortConn);
      }
    } catch (e) {
      if (
        !(
          e instanceof Deno.errors.ConnectionReset ||
          (e instanceof FrameError && e.code === "EOF")
        )
      ) throw e;
    } finally {
      Object.values(consumerTags).flatMap((ch) => Object.values(ch)).forEach((
        abortConsumer,
      ) => abortConsumer.abort());
    }

    async function routeMessage(msg: Message) {
      if (!msg.exchange) {
        if (msg.routingKey) queues[msg.routingKey]?.messages.push(msg);
      } else if (
        exchanges[msg.exchange]?.type === "fanout" || !msg.routingKey
      ) {
        Object.values(exchanges[msg.exchange].route).flat().forEach((
          queueName,
        ) => queues[queueName].messages.push(msg));
      } else {
        exchanges[msg.exchange]?.route[msg.routingKey]?.forEach((queueName) =>
          queues[queueName].messages.push(msg)
        );
      }
      await attemptDelivery();
    }

    async function attemptDelivery() {
      await Promise.all(
        Object.values(queues).map(async (q) => {
          let nextDest = q.lastDelivered + 1;
          nextDest = (nextDest < q.consumers.length ? nextDest : 0) -
            q.consumers.map((c) => c.cancel.aborted).slice(0, nextDest).reduce(
              (acc, aborted) => aborted ? acc + 1 : acc,
              0,
            );
          nextDest = nextDest < q.consumers.length ? nextDest : 0;
          q.consumers = q.consumers.filter((c) => !c.cancel.aborted);
          if (q.consumers.length < 1) return;
          let msg: Message | undefined;
          while ((msg = q.messages.shift()) != undefined) {
            await q.consumers[nextDest++].deliver(msg);
            nextDest = nextDest < q.consumers.length ? nextDest : 0;
          }
          q.lastDelivered = nextDest;
        }),
      );
    }

    async function handleFrame(
      frame: IncomingFrame,
      abortConn: AbortController,
    ) {
      if (frame.type !== "method") return;
      switch (frame.payload.classId) {
        case CONNECTION:
          switch (frame.payload.methodId) {
            case CONNECTION_CLOSE:
              await socket.write([{
                channel: 0,
                type: "method",
                payload: {
                  classId: frame.payload.classId,
                  methodId: CONNECTION_CLOSE_OK,
                  args: {},
                },
              }]);
              socket.close();
              abortConn.abort();
              return;
            case CONNECTION_START_OK:
              await socket.write([{
                channel: 0,
                type: "method",
                payload: {
                  classId: frame.payload.classId,
                  methodId: CONNECTION_TUNE,
                  args: {
                    channelMax: (2 ** (Uint16Array.BYTES_PER_ELEMENT + 1)) - 1,
                  },
                },
              }]);
              return;
            case CONNECTION_TUNE_OK:
              if (frame.payload.args.heartbeat > 0) {
                const heartbeat = setInterval(() => {
                  if (abortConn.signal.aborted) {
                    clearInterval(heartbeat);
                  }
                  socket.write([{
                    channel: 0,
                    type: "heartbeat",
                    payload: new Uint8Array(),
                  }]);
                }, frame.payload.args.heartbeat * 1000);
              }
              return;
            case CONNECTION_OPEN:
              await socket.write([{
                channel: 0,
                type: "method",
                payload: {
                  classId: frame.payload.classId,
                  methodId: CONNECTION_OPEN_OK,
                  args: {},
                },
              }]);
              return;
          }
          break;
        case CHANNEL:
          switch (frame.payload.methodId) {
            case CHANNEL_OPEN:
              // TODO: handle duplicate channel
              if (consumerTags[frame.channel] == undefined) {
                consumerTags[frame.channel] = {};
              }
              await socket.write([{
                channel: frame.channel,
                type: "method",
                payload: {
                  classId: frame.payload.classId,
                  methodId: CHANNEL_OPEN_OK,
                  args: {},
                },
              }]);
              return;
            case CHANNEL_CLOSE_OK:
              return;
          }
          break;
        case QUEUE:
          switch (frame.payload.methodId) {
            case QUEUE_DECLARE: {
              const queueName = frame.payload.args.queue ||
                "amq.gen-" +
                  base64urlencode(crypto.getRandomValues(new Uint8Array(16)));
              if (queues[queueName] == undefined) {
                queues[queueName] = {
                  messages: [],
                  consumers: [],
                  lastDelivered: 0,
                };
              }
              await socket.write([{
                channel: frame.channel,
                type: "method",
                payload: {
                  classId: frame.payload.classId,
                  methodId: QUEUE_DECLARE_OK,
                  args: {
                    queue: queueName,
                    messageCount: queues[queueName].messages.length,
                    consumerCount: queues[queueName].consumers.length,
                  },
                },
              }]);
              return;
            }
            case QUEUE_BIND: {
              if (exchanges[frame.payload.args.exchange] == undefined) {
                await socket.write([{
                  channel: frame.channel,
                  type: "method",
                  payload: {
                    classId: CHANNEL,
                    methodId: CHANNEL_CLOSE,
                    args: {
                      replyCode: SOFT_ERROR_NOT_FOUND,
                      classId: frame.payload.classId,
                      methodId: frame.payload.methodId,
                      replyText:
                        `Exchange '${frame.payload.args.exchange}' not found.`,
                    },
                  },
                }]);
                Object.values(consumerTags[frame.channel]).map((
                  abortConsumer,
                ) => abortConsumer.abort());
                return;
              }
              if (queues[frame.payload.args.queue] == undefined) {
                await socket.write([{
                  channel: frame.channel,
                  type: "method",
                  payload: {
                    classId: CHANNEL,
                    methodId: CHANNEL_CLOSE,
                    args: {
                      replyCode: SOFT_ERROR_NOT_FOUND,
                      classId: frame.payload.classId,
                      methodId: frame.payload.methodId,
                      replyText:
                        `Queue '${frame.payload.args.queue}' not found.`,
                    },
                  },
                }]);
                Object.values(consumerTags[frame.channel]).map((
                  abortConsumer,
                ) => abortConsumer.abort());
                return;
              }
              if (
                exchanges[frame.payload.args.exchange]
                  .route[frame.payload.args.routingKey] == undefined
              ) {
                exchanges[frame.payload.args.exchange]
                  .route[frame.payload.args.routingKey] = [];
              }
              if (
                !exchanges[frame.payload.args.exchange]
                  .route[frame.payload.args.routingKey]
                  .includes(frame.payload.args.queue)
              ) {
                exchanges[frame.payload.args.exchange]
                  .route[frame.payload.args.routingKey]
                  .push(frame.payload.args.queue);
              }
              await socket.write([{
                channel: frame.channel,
                type: "method",
                payload: {
                  classId: frame.payload.classId,
                  methodId: QUEUE_BIND_OK,
                  args: {},
                },
              }]);
              return;
            }
          }
          break;
        case EXCHANGE:
          switch (frame.payload.methodId) {
            case EXCHANGE_DECLARE:
              if (exchanges[frame.payload.args.exchange] == undefined) {
                exchanges[frame.payload.args.exchange] = {
                  type: frame.payload.args.type,
                  route: {},
                };
              }
              await socket.write([{
                channel: frame.channel,
                type: "method",
                payload: {
                  classId: frame.payload.classId,
                  methodId: EXCHANGE_DECLARE_OK,
                  args: {},
                },
              }]);
              return;
          }
          break;
        case BASIC:
          switch (frame.payload.methodId) {
            case BASIC_PUBLISH: {
              const headerFrame = await socket.read();
              if (headerFrame.type !== "header") {
                await handleFrame(headerFrame, abortConn);
                return;
              }
              if (
                headerFrame.channel !== frame.channel ||
                headerFrame.payload.classId !== frame.payload.classId
              ) break;
              if (headerFrame.payload.size === 0) {
                await routeMessage({
                  exchange: frame.payload.args.exchange,
                  routingKey: frame.payload.args.routingKey,
                  props: headerFrame.payload.props,
                  data: new Uint8Array(),
                });
                return;
              }
              const buf = new Buffer();
              while (true) {
                const contentFrame = await socket.read();
                if (contentFrame.type !== "content") {
                  await routeMessage({
                    exchange: frame.payload.args.exchange,
                    routingKey: frame.payload.args.routingKey,
                    props: headerFrame.payload.props,
                    data: buf.bytes(),
                  });
                  await handleFrame(contentFrame, abortConn);
                  return;
                }
                if (contentFrame.channel !== frame.channel) break;
                await buf.write(contentFrame.payload);
                if (buf.length >= headerFrame.payload.size) {
                  await routeMessage({
                    exchange: frame.payload.args.exchange,
                    routingKey: frame.payload.args.routingKey,
                    props: headerFrame.payload.props,
                    data: buf.bytes(),
                  });
                  return;
                }
              }
              return;
            }
            case BASIC_CONSUME: {
              const consumerTag = frame.payload.args.consumerTag ||
                "amq.ctag-" +
                  base64urlencode(crypto.getRandomValues(new Uint8Array(16)));
              const abortConsumer = new AbortController();
              const classId = frame.payload.classId;
              queues[frame.payload.args.queue].consumers.push({
                deliver: async (msg) => {
                  await socket.write([{
                    channel: frame.channel,
                    type: "method",
                    payload: {
                      classId,
                      methodId: BASIC_DELIVER,
                      args: {
                        consumerTag,
                        deliveryTag: 0, // TODO proper handling of delivery tag
                        exchange: msg.exchange,
                        routingKey: msg.routingKey,
                      },
                    },
                  }, {
                    channel: frame.channel,
                    type: "header",
                    payload: {
                      classId,
                      props: msg.props,
                      size: msg.data.length,
                    },
                  }, {
                    channel: frame.channel,
                    type: "content",
                    payload: msg.data,
                  }]);
                },
                cancel: abortConsumer.signal,
              });
              if (consumerTags[frame.channel][consumerTag]) {
                await socket.write([{
                  channel: frame.channel,
                  type: "method",
                  payload: {
                    classId: CONNECTION,
                    methodId: CONNECTION_CLOSE,
                    args: {
                      replyCode: HARD_ERROR_NOT_ALLOWED,
                      classId: BASIC,
                      methodId: BASIC_CONSUME,
                      replyText: `Cannot reuse consumer tag '${consumerTag}'.`,
                    },
                  },
                }]);
                socket.close();
                abortConn.abort();
                return;
              }
              consumerTags[frame.channel][consumerTag] = abortConsumer;
              await socket.write([{
                channel: frame.channel,
                type: "method",
                payload: {
                  classId: frame.payload.classId,
                  methodId: BASIC_CONSUME_OK,
                  args: { consumerTag },
                },
              }]);
              await attemptDelivery();
              return;
            }
            case BASIC_CANCEL: {
              const abortConsumer =
                consumerTags[frame.channel][frame.payload.args.consumerTag];
              if (abortConsumer) {
                abortConsumer.abort();
                delete consumerTags[frame.channel][
                  frame.payload.args.consumerTag
                ];
              }
              await socket.write([{
                channel: frame.channel,
                type: "method",
                payload: {
                  classId: frame.payload.classId,
                  methodId: BASIC_CANCEL_OK,
                  args: {
                    consumerTag: frame.payload.args.consumerTag,
                  },
                },
              }]);
              return;
            }
            case BASIC_ACK:
              return;
          }
          break;
      }
      console.log("unhandled frame", frame);
    }
  }
}

function uint8ArrayEqual(a: Uint8Array, b: Uint8Array) {
  return a === b
    ? true
    : a == null || b == null
    ? false
    : a.length !== b.length
    ? false
    : a.every((val, idx) => val === b[idx]);
}

if (import.meta.main) main();
