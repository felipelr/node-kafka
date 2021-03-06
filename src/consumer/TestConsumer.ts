import {
  Consumer,
  ConsumerSubscribeTopics,
  EachBatchPayload,
  Kafka,
  EachMessagePayload,
} from "kafkajs";

export default class TestConsumer {
  private kafkaConsumer: Consumer;

  public constructor(clientId: string) {
    this.kafkaConsumer = this.createKafkaConsumer(clientId);
  }

  public async startConsumer(subscribeTopics: string[]): Promise<void> {
    const topics: ConsumerSubscribeTopics = {
      topics: subscribeTopics,
      fromBeginning: false,
    };

    try {
      await this.kafkaConsumer.connect();
      await this.kafkaConsumer.subscribe(topics);

      await this.kafkaConsumer.run({
        eachMessage: async (messagePayload: EachMessagePayload) => {
          const { topic, partition, message } = messagePayload;
          const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
          console.log(`- ${prefix} ${message.key}#${message.value}`);
        },
      });
    } catch (error) {
      console.log("Error: ", error);
    }
  }

  public async startBatchConsumer(subscribeTopics: string[]): Promise<void> {
    const topics: ConsumerSubscribeTopics = {
      topics: subscribeTopics,
      fromBeginning: false,
    };

    try {
      await this.kafkaConsumer.connect();
      await this.kafkaConsumer.subscribe(topics);
      await this.kafkaConsumer.run({
        eachBatch: async (eatchBatchPayload: EachBatchPayload) => {
          const { batch } = eatchBatchPayload;
          for (const message of batch.messages) {
            const prefix = `${batch.topic}[${batch.partition} | ${message.offset}] / ${message.timestamp}`;
            console.log(`- ${prefix} ${message.key}#${message.value}`);
          }
        },
      });
    } catch (error) {
      console.log("Error: ", error);
    }
  }

  public async shutdown(): Promise<void> {
    await this.kafkaConsumer.disconnect();
  }

  private createKafkaConsumer(clientId: string): Consumer {
    const kafka = new Kafka({
      clientId: clientId,
      brokers: ["broker:29092"],
    });
    const consumer = kafka.consumer({ groupId: "consumer-group" });
    return consumer;
  }
}
