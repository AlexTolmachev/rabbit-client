# rabbit-client

RabbitMQ client with implemented reconnection & error handling

## Install
```shell
$ npm i rabbit-client
```

## Test
```shell
$ npm test
```

## API

### Constructor
```js
const RabbitClient = require('rabbit-client');

const rabbitClient = new RabbitClient('amqp://localhost:5672', { json: true, appName: 'my-app' });
```

Pass `{ json: true }` if you want to have JSON.parsed messages in `data` field of consume callback function.

### Get connection
#### .getConnection()
```js
rabbitClient.getConnection();
```

### Get channel
#### .getChannel()
```js
const channel = await rabbitClient.getChannel();

await channel.sendToQueue('my-queue', { foo: bar });
```

You can use objects as content without converting it to Buffer.

### Get channel
#### .getChannel(options)
```js
rabbitClient.getChannel({
  onReconnect: async (channel) => {
    await channel.assertQueue('my-queue');
    await channel.purgeQueue('my-queue');
    await channel.prefetch(10);
    await channel.consume('my-queue', (msg, ch, data) => {
      console.log(data); // parsed json
      ch.ack(msg);
    })
  }
});
```
