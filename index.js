const mysql = require("mysql2");
const dotenv = require("dotenv");

dotenv.config();
let conection = null;

async function connect() {
  try {
    conection = mysql.createConnection({
      host: process.env.DB_HOST?.toString(),
      port: Number(process.env.DB_PORT?.toString()),
      user: process.env.DB_USER?.toString(),
      password: process.env.DB_PASS?.toString(),
      database: process.env.DB_NAME?.toString(),
    });
    if (conection) {
      console.log("====================================================")
      console.log("connection db success");
      console.log("====================================================")
    }
  } catch (error) {
    console.log("====================================================")
    console.log("connection error: ", error);
    console.log("====================================================")
  }
}

async function executeQuery(query) {
  try {
    const result = await conection.promise().query(query);
    return result;
  } catch (error) {
    console.log("====================================================")
    console.log("error in execute query ", error.sqlMessage);
    console.log("====================================================")
    return error.sqlMessage;
  }
}
async function rollback() {
  try {
    const result = await conection.promise().query("rollback");
    return result;
  } catch (error) {
    console.log("====================================================")
    console.log("error in rollback", error);
    console.log("====================================================")
  }
}
async function inserMasiveClient(event) {
  const errorControll = [];

  for (let i = 0; i < event.clients.length; i++) {
    const client = event.clients[i];
    const sql = `INSERT INTO client (NIT , PROJECT_ID , CLIENT_NAME, BUSSINESS_NAME, 
        PHONE, RISK,CONDITION_PAYMENT, EMAIL, RADICATION_TYPE, HOLDING_ID, CLIENT_TYPE_ID, DOCUMENT_TYPE, LOCATIONS, CLIENT_STATUS, BILLING_PERIOD) VALUES (
            '${client.nit}',
            '${client.project}',
            '${client.name}',
            '${client.bussines_name}',
            '${client.phone}',
            'NO CALCULADO',
            'CONDICION DE PAGO',
            '${client.email}',
            '${client.radication_type}',
            ${client.holding ? client.holding : null},
            '${client.client_type}',
            '${client.document_type}',
            '${JSON.stringify(client.locations)}',
            'CREADO',


            '${client.billling_period}'
        )`;
    try {
      const result = await executeQuery(sql);
      if (typeof result === 'string'){
         throw new Error(result);
      }
      console.log(`client ${client.nit} update success`);
    } catch (error) {
      console.log("====================================================")
      console.log('error' , error)
      console.log("====================================================")
       errorControll.push({
        nit: client.nit,
        message: error.message ? error.message : 'lambda failed',
      });
    }
  }
  return errorControll;
}

exports.handler = async function (event) {
    await connect();
  const result = await inserMasiveClient(event);
  if (result.length > 0) {
    await rollback();
    return {
      statusCode: 500,
      body: {
        message: "error in user insert",
        errors: result,
      },
    };
  }
  return {
    statusCode: 200,
    body: {
      message: "success",
    },
  };
};
