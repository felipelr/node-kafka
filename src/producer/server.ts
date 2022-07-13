import express, { Request, Response, NextFunction } from "express";
import ProducerFactory from "./factory/ProducerFactory";
import homeRouter from "./routes/homeRouter";

const port = process.env.PORT || 3000;
const app = express();
const producerFactory = new ProducerFactory("producer-nodejs-kafka");

declare global {
  namespace Express {
    interface Request {
      producerFactory: ProducerFactory;
    }
  }
}

app.use(express.json());

app.use((req: Request, res: Response, next: NextFunction) => {
  req.producerFactory = producerFactory;
  next();
});

app.use("/", homeRouter);

const run = async () => {
  await producerFactory.start();
  app.listen(port, () => {
    console.log("servidor rodando");
  });
};

run().catch((e) => {
  console.error(`[example/producer] ${e.message}`, e);
  producerFactory.shutdown();
});
