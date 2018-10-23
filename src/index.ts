import * as mysql from "mysql";

export type FieldType = string | number | Date | boolean;
export type SqlParams = Array<FieldType | FieldType[][]>;
export type MySQLOrMySQL2Lib = any;
export type MySQLPoolConfig = any;

export const enum Types {
  DECIMAL = 0x00, // aka DECIMAL (http://dev.mysql.com/doc/refman/5.0/en/precision-math-decimal-changes.html)
  TINY = 0x01, // aka TINYINT, 1 byte
  SHORT = 0x02, // aka SMALLINT, 2 bytes
  LONG = 0x03, // aka INT, 4 bytes
  FLOAT = 0x04, // aka FLOAT, 4-8 bytes
  DOUBLE = 0x05, // aka DOUBLE, 8 bytes
  NULL = 0x06, // NULL (used for prepared statements, I think)
  TIMESTAMP = 0x07, // aka TIMESTAMP
  LONGLONG = 0x08, // aka BIGINT, 8 bytes
  INT24 = 0x09, // aka MEDIUMINT, 3 bytes
  DATE = 0x0a, // aka DATE
  TIME = 0x0b, // aka TIME
  DATETIME = 0x0c, // aka DATETIME
  YEAR = 0x0d, // aka YEAR, 1 byte (don't ask)
  NEWDATE = 0x0e, // aka ?
  VARCHAR = 0x0f, // aka VARCHAR (?)
  BIT = 0x10, // aka BIT, 1-8 byte
  TIMESTAMP2 = 0x11, // aka TIMESTAMP with fractional seconds
  DATETIME2 = 0x12, // aka DATETIME with fractional seconds
  TIME2 = 0x13, // aka TIME with fractional seconds
  JSON = 0xf5, // aka JSON
  NEWDECIMAL = 0xf6, // aka DECIMAL
  ENUM = 0xf7, // aka ENUM
  SET = 0xf8, // aka SET
  TINY_BLOB = 0xf9, // aka TINYBLOB, TINYTEXT
  MEDIUM_BLOB = 0xfa, // aka MEDIUMBLOB, MEDIUMTEXT
  LONG_BLOB = 0xfb, // aka LONGBLOG, LONGTEXT
  BLOB = 0xfc, // aka BLOB, TEXT
  VAR_STRING = 0xfd, // aka VARCHAR, VARBINARY
  STRING = 0xfe, // aka CHAR, BINARY
  GEOMETRY = 0xff, // aka GEOMETRY
}

export interface FieldInfo {
  catalog: string;
  db: string;
  table: string;
  orgTable: string;
  name: string;
  orgName: string;
  charsetNr: number;
  length: number;
  type: Types;
  flags: number;
  decimals: number;
  default?: string;
  zeroFill: boolean;
  protocol41: boolean;
}

export interface QueryResult {
  results: any;
  fields?: FieldInfo[];
}

export interface SelectResult<T> {
  rows: T[];
  fields?: FieldInfo[];
}

export interface AffectedResult {
  affectedRows: number;
}

export interface InsertResult extends AffectedResult {
  insertId: string | number; // make sense when pk is auto increment, string for big number.
}

export interface UpdateResult extends AffectedResult {
  changedRows: number;
}

export type DeleteResult = AffectedResult;

export interface Connection {
  query(sql: string, params?: SqlParams): Promise<QueryResult>;
  select<T>(sql: string, params?: SqlParams): Promise<SelectResult<T>>;
  insert(sql: string, params?: SqlParams): Promise<InsertResult>;
  update(sql: string, params?: SqlParams): Promise<UpdateResult>;
  delete(sql: string, params?: SqlParams): Promise<DeleteResult>;
  del(sql: string, params?: SqlParams): Promise<DeleteResult>;
}

export interface PoolSugar extends Connection {
  getPool(): any;
  withConnection<T>(
    func: (connection: Connection) => Promise<T>,
    withTransaction?: boolean,
  ): Promise<T>;
  destroy(): void;
}

function promisify(func: (...args: any[]) => any, paramLength?: number) {
  return (...args: any[]) => {
    return new Promise<any>((f, r) => {
      const list: any[] = [];
      for (let i = 0; i < (paramLength || func.length - 1); i++) {
        list.push(args[i]);
      }
      list.push((error: any, result: any) => {
        if (error) { r(error); } else { f(result); }
      });
      func.apply(null, list);
    });
  };
}

function wrapConnection(that: mysql.Connection): Connection {

  const query = (sql: string, params?: SqlParams): Promise<QueryResult> => {
    return new Promise<QueryResult>((f, r) => {
      that.query(sql, params, (err, results, fields: any): void => {
        if (err) { r(err); } else { f({ results, fields }); }
      });
    });
  };

  const cud = (sql: string, params?: SqlParams): Promise<any> => {
    return query(sql, params).then((result) => result.results);
  };

  return {
    query,
    select: <T>(sql: string, params?: SqlParams): Promise<SelectResult<T>> => {
      return query(sql, params).then((result) => ({
        rows: result.results,
        fields: result.fields,
      }));
    },
    insert: cud,
    update: cud,
    delete: cud,
    del: cud,
  };
}

function createPool(
  lib: MySQLOrMySQL2Lib,
  config: MySQLPoolConfig,
): PoolSugar {
  let pool: mysql.Pool | undefined = lib.createPool(config);
  function withConnection<T>(
    func: (connection: Connection) => Promise<T>,
    withTransaction: boolean = false,
  ): Promise<T> {
    return new Promise((f, r) => {
      if (!pool) { return r(new Error("Connection pool already destroyed.")); }
      pool.getConnection(async (err, connection) => {
        if (err) { return r(err); }
        let result: T | undefined;
        try {
          if (withTransaction) { await promisify(connection.beginTransaction, 0)(); }
          result = await func(wrapConnection(connection));
        } catch (e) {
          err = e;
        }
        try {
          if (err) {
            if (withTransaction) { await promisify(connection.rollback, 0)(); }
            r(err);
          } else {
            if (withTransaction) { await promisify(connection.commit, 0)(); }
            f(result);
          }
        } catch (e) {
          if (err) {
            e.lastError = err;
          }
          r(e);
        }
        connection.release();
      });
    });
  }
  return {
    getPool() { return pool; },
    destroy() {
      if (!pool) { return; }
      pool.end();
      pool = undefined;
    },
    withConnection,
    query: (
      sql: string, params?: SqlParams,
    ): Promise<QueryResult> => withConnection(
      (connection) => connection.query(sql, params),
    ),
    select: <T>(
      sql: string, params?: SqlParams,
    ): Promise<SelectResult<T>> => withConnection(
      (connection) => connection.select<T>(sql, params),
    ),
    insert: (
      sql: string, params?: SqlParams,
    ): Promise<InsertResult> => withConnection(
      (connection) => connection.insert(sql, params),
    ),
    update: (
      sql: string, params?: SqlParams,
    ): Promise<UpdateResult> => withConnection(
      (connection) => connection.update(sql, params),
    ),
    delete: (
      sql: string, params?: SqlParams,
    ): Promise<DeleteResult> => withConnection(
      (connection) => connection.delete(sql, params),
    ),
    del: (
      sql: string, params?: SqlParams,
    ): Promise<DeleteResult> => withConnection(
      (connection) => connection.delete(sql, params),
    ),
  };
}

export {
  createPool,
};
