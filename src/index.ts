import * as mysql from 'mysql';

export type FieldType = string | number | Date | Boolean;
export { Pool, PoolConfig, MysqlError } from 'mysql';

export interface Connection {
  query<T>(sql: string, params?: (FieldType | FieldType[][])[]): Promise<T[]>;
}

export interface PoolSugar extends Connection {
  getPool(): mysql.Pool | undefined;
  withConnection<T>(
    func: (connection: Connection) => Promise<T>,
    withTransaction?: boolean
  ): Promise<T>;
  destroy(): void;
}

function promisify(func: (...args: any[]) => any, paramLength?: number) {
  return (...args: any[]) => {
    return new Promise<any>((f, r) => {
      let list: any[] = [];
      for (let i = 0; i < (paramLength || func.length - 1); i++) {
        list.push(args[i]);
      }
      list.push(function (error: any, result: any) {
        if (error) r(error);
        else f(result);
      });
      func.apply(null, list);
    });
  }
}

function wrapConnection(that: mysql.Connection): Connection {
  return {
    query: promisify(that.query.bind(that), 2)
  }
}

function createPool(
  lib: typeof mysql,
  config: mysql.PoolConfig | string
): PoolSugar {
  let pool: mysql.Pool | undefined = lib.createPool(config);
  function withConnection<T>(
    func: (connection: Connection) => Promise<T>,
    withTransaction: boolean = false
  ): Promise<T> {
    return new Promise((f, r) => {
      if (!pool) return r(new Error('Connection pool already destroyed.'));
      pool.getConnection(async function (err, connection) {
        if (err) return r(err);
        let result: T | undefined;
        try {
          if (withTransaction) await promisify(connection.beginTransaction, 0)();
          result = await func(wrapConnection(connection));
        } catch (e) {
          err = e;
        }
        try {
          if (err) {
            if (withTransaction) await promisify(connection.rollback, 0)();
            r(err);
          } else {
            if (withTransaction) await promisify(connection.commit, 0)();
            f(result);
          }
        } catch (e) {
          if (err) {
            e.lastError = err;
          }
          r(e);
        }
      });
    });
  }
  async function query<T>(
    sql: string, params?: (FieldType | FieldType[][])[]
  ): Promise<T[]> {
    return withConnection(connection => {
      return connection.query<T>(sql, params);
    });
  }
  return {
    getPool() { return pool; },
    destroy() { pool = undefined; },
    query, withConnection
  };
}

export {
  createPool
}
