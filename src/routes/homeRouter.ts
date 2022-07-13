import { Router, Request, Response } from "express";
import { CustomMessageFormat } from "../producer/factory/ProducerFactory";

const homeRouter = Router();

homeRouter.get("/", async (request: Request, response: Response) => {
  response.status(200).json({
    message: "olá você está na home",
  });
});

homeRouter.post("/", async (request: Request, response: Response) => {
  try {
    const { name, email } = request.body;
    const message = { name, email } as CustomMessageFormat;
    const messages = [message];

    await request.producerFactory.sendBatch("sales-topic", messages);

    response.status(200).json({
      message: "mensagens enviadas",
      data: messages,
    });
  } catch (err: any) {
    response.status(500).json({
      message: err.message || "erro ao enviar mensagens",
    });
  }
});

export default homeRouter;
