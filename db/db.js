import { createClient } from "@libsql/client";
import { createProducer } from "../klite/src/index.js";

export const db = createClient({
  url: "file:./db.sqlite",
});

export const analyticsProducer = createProducer({ db });
