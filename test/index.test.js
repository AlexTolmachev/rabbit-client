const { expect } = require('chai');
const RabbitClient = require('../src');
const { sleep } = require('../src/utils');

const { RABBIT_URL } = process.env;

describe('RabbitConnection', () => {
  it('should be instance of RabbitConnection', async () => {
    const client = new RabbitClient();

    expect(client).to.be.instanceOf(RabbitClient);
  });

  it('should throw error when cannot getConnection 3 times', async () => {
    const client = new RabbitClient('incorrectUrl', {
      attempts: 3,
    });

    const startTime = Date.now();

    try {
      await client.getConnection();
    } catch (err) {
      expect(err.message).to.be.eql('RabbitMQ: Connection attempts count exceeded');
      expect(Date.now() - startTime).to.be.above(200).and.below(250);

      return;
    }

    expect(false).to.be.eql(true, 'Expected to throw error');
  });

  it('should throw error when cannot getChannel 3 times', async () => {
    const client = new RabbitClient('incorrectUrl', {
      attempts: 3,
    });

    const startTime = Date.now();

    try {
      await client.getChannel();
    } catch (err) {
      expect(err.message).to.be.eql('RabbitMQ: Connection attempts count exceeded');
      expect(Date.now() - startTime).to.be.above(200).and.below(250);

      return;
    }

    expect(false).to.be.eql(true, 'Expected to throw error');
  });

  it('should send messages', async () => {
    const testMessage = 'test message';
    const testQueue = 'testQueue';

    const client = new RabbitClient(RABBIT_URL);
    const channel = await client.getChannel();

    await channel.assertQueue(testQueue);
    await channel.purgeQueue(testQueue);
    await channel.sendToQueue(testQueue, Buffer.from(testMessage));

    const message = await channel.get(testQueue);

    expect(message.content.toString()).to.be.eql(testMessage);

    channel.ack(message);
  });

  it('should consume messages', async () => {
    const testMessage = 'test message';
    const testQueue = 'testQueue';

    const client = new RabbitClient(RABBIT_URL);
    const channel = await client.getChannel();

    await channel.assertQueue(testQueue);
    await channel.purgeQueue(testQueue);
    await channel.sendToQueue(testQueue, Buffer.from(testMessage));

    await Promise.race([
      new Promise((resolve) => {
        channel.consume(testQueue, async (msg, ch) => {
          expect(msg.content.toString()).to.be.eql(testMessage);

          ch.ack(msg);
          await sleep(100);
          await ch.cancel(msg.fields.consumerTag);

          resolve();
        });
      }),
      sleep(1000).then(() => expect(false).to.be.eql(true, 'Expected to consume message')),
    ]);
  });

  it('should return undefined 3rd argument if json:false', async () => {
    const testMessage = 'test message';
    const testQueue = 'testQueue';

    const client = new RabbitClient(RABBIT_URL);
    const channel = await client.getChannel();

    await channel.assertQueue(testQueue);
    await channel.purgeQueue(testQueue);
    await channel.sendToQueue(testQueue, Buffer.from(testMessage));

    await Promise.race([
      new Promise(async (resolve) => {
        const consumePromise = channel
          .consume(testQueue, async (msg, ch, data) => {
            // Just for fun
            const { consumerTag } = await consumePromise;

            expect(msg.content.toString()).to.be.eql(testMessage);
            expect(data).to.be.eql(testMessage);

            ch.ack(msg);
            ch.cancel(consumerTag);

            resolve();
          });
      }),
      sleep(1000).then(() => expect(false).to.be.eql(true, 'Expected to consume message')),
    ]);
  });

  it('should return object 3rd argument if json:true', async () => {
    const testMessage = JSON.stringify({
      foo: 'bar',
    });
    const testQueue = 'testQueue';

    const client = new RabbitClient(RABBIT_URL, { json: true });
    const channel = await client.getChannel();

    await channel.assertQueue(testQueue);
    await channel.purgeQueue(testQueue);
    await channel.sendToQueue(testQueue, Buffer.from(testMessage));

    await Promise.race([
      new Promise((resolve) => {
        channel.consume(testQueue, async (msg, ch, data) => {
          expect(msg.content.toString()).to.be.eql(testMessage);
          expect(data).to.be.eql(JSON.parse(testMessage));

          await ch.ack(msg);
          await ch.cancel(msg.fields.consumerTag);

          resolve();
        });
      }),
      sleep(1000).then(() => expect(false).to.be.eql(true, 'Expected to consume message')),
    ]);
  });

  it('should allow string as message content', async () => {
    const testMessage = 'some string';
    const testQueue = 'testQueue';

    const client = new RabbitClient(RABBIT_URL);
    const channel = await client.getChannel();

    await channel.assertQueue(testQueue);
    await channel.purgeQueue(testQueue);
    await channel.sendToQueue(testQueue, testMessage);

    await Promise.race([
      new Promise((resolve) => {
        channel.consume(testQueue, async (msg, ch, data) => {
          expect(msg.content.toString()).to.be.eql(testMessage);
          expect(data).to.be.eql(testMessage);

          await ch.ack(msg);
          await ch.cancel(msg.fields.consumerTag);

          resolve();
        });
      }),
      sleep(1000).then(() => expect(false).to.be.eql(true, 'Expected to consume message')),
    ]);
  });

  it('should allow number as message content', async () => {
    const testMessage = 123;
    const testQueue = 'testQueue';

    const client = new RabbitClient(RABBIT_URL);
    const channel = await client.getChannel();

    await channel.assertQueue(testQueue);
    await channel.purgeQueue(testQueue);
    await channel.sendToQueue(testQueue, testMessage);

    await Promise.race([
      new Promise((resolve) => {
        channel.consume(testQueue, async (msg, ch, data) => {
          expect(msg.content.toString()).to.be.eql(testMessage.toString());
          expect(data).to.be.eql(testMessage.toString());

          await ch.ack(msg);
          await ch.cancel(msg.fields.consumerTag);

          resolve();
        });
      }),
      sleep(1000).then(() => expect(false).to.be.eql(true, 'Expected to consume message')),
    ]);
  });

  it('should allow object as message content', async () => {
    const testMessage = {
      foo: 'bar',
    };
    const testQueue = 'testQueue';

    const client = new RabbitClient(RABBIT_URL, { json: true });
    const channel = await client.getChannel();

    await channel.assertQueue(testQueue);
    await channel.purgeQueue(testQueue);
    await channel.sendToQueue(testQueue, testMessage);

    await Promise.race([
      new Promise((resolve) => {
        channel.consume(testQueue, async (msg, ch, data) => {
          expect(msg.content.toString()).to.be.eql(JSON.stringify(testMessage));
          expect(data).to.be.eql(testMessage);

          await ch.ack(msg);
          await ch.cancel(msg.fields.consumerTag);

          resolve();
        });
      }),
      sleep(1000).then(() => expect(false).to.be.eql(true, 'Expected to consume message')),
    ]);
  });

  it('should consume using onReconnect function', async () => {
    const testMessage = {
      foo: 'bar',
    };
    const testQueue = 'testQueue';

    await Promise.race([
      new Promise(async (resolve) => {
        const client = new RabbitClient(RABBIT_URL, { json: true });

        await client.getChannel({
          async onReconnect(channel) {
            await channel.assertQueue(testQueue);
            await channel.purgeQueue(testQueue);
            await channel.sendToQueue(testQueue, testMessage);

            await channel.consume(testQueue, async (msg, ch, data) => {
              expect(msg.content.toString()).to.be.eql(JSON.stringify(testMessage));
              expect(data).to.be.eql(testMessage);

              await ch.ack(msg);
              await ch.cancel(msg.fields.consumerTag);

              resolve();
            });
          },
        });
      }),
      sleep(1000).then(() => expect(false).to.be.eql(true, 'Expected to consume message')),
    ]);
  });
});
