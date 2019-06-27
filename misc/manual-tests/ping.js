const RabbitClient = require('../../src');

const { RABBIT_URL } = process.env;

const startTest = async () => {
  const testMessage = 'test message';
  const testQueue = 'testQueue';

  const client = new RabbitClient(RABBIT_URL);

  setInterval(async () => {
    const channel = await client.getChannel();

    await channel.assertQueue(testQueue);
    await channel.sendToQueue(testQueue, Buffer.from(testMessage));
    await channel.close();
    console.log(new Date(), '-->');
  }, 10000);

  await client.getChannel({
    async onReconnect(channel) {
      console.log(new Date(), 'CONNECTED');

      await channel.consume(testQueue, (msg, ch) => {
        console.log(new Date(), '<--', msg);
        ch.ack(msg);
      });
    },
  });
};

startTest();
