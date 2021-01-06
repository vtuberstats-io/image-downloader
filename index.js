const KAFKA_BROKERS = process.env.KAFKA_BROKERS;
const MONGODB_URL = process.env.MONGODB_URL;
const IMAGE_CACHE_TIME_MS = Number(process.env.IMAGE_CACHE_TIME_MS) || 1000 * 60 * 60 * 24 * 3; // 3 days
const DATA_ROOT_PATH = process.env.DATA_ROOT_PATH;
const __externalUrl = process.env.EXTERNAL_URL;
const HOSTNAME = process.env.HOSTNAME; // offered by kubernetes automatically

if (!KAFKA_BROKERS || !MONGODB_URL || !DATA_ROOT_PATH || !__externalUrl || !HOSTNAME) {
  console.error(`missing environment variables, env: ${JSON.stringify(process.env)}`);
  process.exit(1);
}

const EXTERNAL_URL = __externalUrl.endsWith('/')
  ? __externalUrl.substr(0, __externalUrl.length - 1)
  : __externalUrl;

const { addExitHook } = require('exit-hook-plus');
const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const cryptoRandomString = require('crypto-random-string');

const kafka = new Kafka({
  clientId: HOSTNAME,
  brokers: KAFKA_BROKERS.trim().split(',')
});
const consumer = kafka.consumer({ groupId: 'image-downloader', sessionTimeout: 12 * 1000 });
const mongo = new MongoClient(MONGODB_URL, { useUnifiedTopology: true });

async function init() {
  console.info('connecting to kafka');
  await consumer.connect();
  await consumer.subscribe({ topic: 'download-image' });
  addExitHook(async () => await consumer.disconnect());

  console.info('connecting to mongodb');
  await mongo.connect();
  addExitHook(async () => await mongo.close());
  const db = mongo.db('vtuberstats');
  const downloadedImageCollection = db.collection('downloaded-image');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const value = JSON.parse(message.value.toString());

      const image = await downloadedImageCollection.findOne({ originalUrl: value.data.url });
      if (image) {
        if (Date.now() - new Date(image.lastCachedAt).getTime() < IMAGE_CACHE_TIME_MS) {
          console.info(`skipped downloading image ${image.originalUrl} because it's up-to-date`);
          return;
        }
        console.info(
          `out-dated image ${image.originalUrl} (last cached at ${image.lastCachedAt}), now try to re-download it`
        );
      }

      try {
        const fullName = await downloadImage(value.url, cryptoRandomString({ length: 32 }));
        const cachedUrl = `${EXTERNAL_URL}/${fullName}`;

        await downloadedImageCollection.updateOne(
          { originalUrl: value.data.url },
          {
            $set: {
              domain: value.meta.domain,
              type: value.meta.type,
              originalUrl: value.data.url,
              cachedUrl,
              lastCachedAt: new Date().toISOString()
            }
          },
          { upsert: true }
        );
      } catch (e) {
        if (image) {
          console.error(
            `skipped re-downloading pre-existed image ${value.url} due to error:`,
            e.stack
          );
        } else {
          console.error(`error downloading image ${value.url}`, e.stack);
          throw e;
        }
      }
    }
  });
}

const CONTENT_TYPE_MAPPING = {
  'image/webp': 'webp',
  'image/png': 'png',
  'image/jpeg': 'jpg',
  'image/gif': 'gif'
};

function downloadImage(url, name) {
  return axios.get(url, { responseType: 'stream' }).then((response) => {
    const contentType = response.headers['content-type'];
    const extensionName = CONTENT_TYPE_MAPPING[contentType];
    if (!extensionName) {
      throw new Error(`unknown content-type ${contentType} for image ${url}`);
    }

    const fullName = `${name}.${extensionName}`;
    const imagePath = path.join(DATA_ROOT_PATH, fullName);
    response.data.pipe(fs.createWriteStream(imagePath));

    return fullName;
  });
}

init();
