import { RDP03_Nombres_Tablas } from "./RDP03_Tablas";

// Interfaz para operaciones MongoDB
export interface MongoOperation {
  operation:
    | "find"
    | "findOne"
    | "insertOne"
    | "insertMany"
    | "updateOne"
    | "updateMany"
    | "deleteOne"
    | "deleteMany"
    | "replaceOne"
    | "aggregate"
    | "countDocuments";
  collection: RDP03_Nombres_Tablas;
  filter?: any;
  data?: any;
  options?: any;
  pipeline?: any[];
}