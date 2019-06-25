const amqplib = require('amqplib');
const nodeCleanup = require('node-cleanup');
const { sleep, convertToBuffer } = require('./utils');

const defaultConstructorOptions = {
  json: false,
  attempts: 3000,
  sleepTime: 100,
};

module.exports = class RabbitClient {
  constructor(rabbitUrl = 'amqp://localhost:5672', options) {
    this.rabbitUrl = rabbitUrl;
    this.connection = null;

    this.options = {
      ...defaultConstructorOptions,
      ...options,
    };
  }

  log(type, ...args) {
    if (this.options.disableLogging) {
      return;
    }

    console[type](...args);
  }

  async connect() {
    this.connection = await amqplib.connect(
      this.rabbitUrl,
      this.options.appName ? { clientProperties: { APP_NAME: this.options.appName } } : undefined,
    );

    this.connection.on('error', async (e) => {
      this.connection.close().catch(this.log.bind(this, 'error'));

      this.log('error', 'RabbitMQ: Connection error', e);
      this.connection = null;
      this.connect().catch(this.log.bind(this, 'error'));
    });

    this.connection.on('close', () => {
      this.log('error', 'RabbitMQ: Connection close');
      this.connection = null;
      this.connect().catch(this.log.bind(this, 'error'));
    });

    nodeCleanup(() => this.connection.close().catch(this.log.bind(this, 'error')));
  }

  async getConnection(attempts = this.options.attempts) {
    if (!this.connection) {
      try {
        await this.connect();
      } catch (e) {
        if (attempts === 1) {
          throw new Error('RabbitMQ: Connection attempts count exceeded');
        }

        await sleep(this.options.sleepTime);

        return this.getConnection(attempts - 1);
      }
    }

    return this.connection;
  }

  async getChannel(options = {}) {
    try {
      const connection = await this.getConnection();
      const channel = await connection.createChannel();

      const sendToQueue = channel.sendToQueue.bind(channel);

      channel.sendToQueue = (queue, content, sendToQueueOptions) => sendToQueue(
        queue, convertToBuffer(content), sendToQueueOptions,
      );

      const publish = channel.publish.bind(channel);

      channel.publish = (exchange, routingKey, content, publishOptions) => publish(
        exchange, routingKey, convertToBuffer(content), publishOptions,
      );

      const consume = channel.consume.bind(channel);

      channel.consume = (queueName, onMessage, consumeOptions) => consume(
        queueName,
        (msg) => {
          if (msg === null) {
            return;
          }

          const data = msg.content.toString();

          if (this.options.json === true) {
            try {
              const json = JSON.parse(data);

              onMessage(msg, channel, json);

              return;
            } catch (err) {
              onMessage(msg, channel, null);

              return;
            }
          }

          onMessage(msg, channel, data);
        },
        consumeOptions,
      );

      if (typeof options.onReconnect === 'function') {
        channel.on('error', async (e) => {
          channel.close().catch(this.log.bind(this, 'error'));

          this.log('error', 'RabbitMQ: Channel error', e);

          await sleep(this.options.sleepTime);
          await this.getChannel(options);
        });

        channel.on('close', async () => {
          this.log('error', 'RabbitMQ: Channel close');

          await sleep(this.options.sleepTime);
          await this.getChannel(options);
        });

        await options.onReconnect(channel);
      }

      return channel;
    } catch (err) {
      this.log('error', 'RabbitMQ: Cannot create channel', err);
      throw err;
    }
  }
};
