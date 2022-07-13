import {
  Kafka,
  logCreator,
  logLevel,
  Producer,
  ProducerBatch,
  Message,
  TopicMessages,
} from "kafkajs";

export interface CustomMessageFormat {
  name: string;
  email: string;
}

export default class ProducerFactory {
  private producer: Producer;

  constructor(clientId: string) {
    this.producer = this.createProducer(clientId);
  }

  public async start(): Promise<void> {
    try {
      await this.producer.connect();
    } catch (error) {
      console.log("Error connecting the producer: ", error);
    }
  }

  public async shutdown(): Promise<void> {
    await this.producer.disconnect();
  }

  public async sendBatch(
    topicName: string,
    messages: Array<CustomMessageFormat>,
    messageKey?: string
  ): Promise<void> {
    const kafkaMessages: Array<Message> = messages.map((message) => {
      return {
        key: messageKey,
        value: JSON.stringify(message),
      };
    });

    const topicMessages: TopicMessages = {
      topic: topicName,
      messages: kafkaMessages,
    };

    const batch: ProducerBatch = {
      topicMessages: [topicMessages],
    };

    await this.producer.sendBatch(batch);
  }

  private createProducer(clientId: string): Producer {
    const kafka = new Kafka({
      clientId: clientId,
      brokers: ["localhost:9092"],
    });

    return kafka.producer();
  }
}
