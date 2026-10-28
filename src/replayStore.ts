import { constants } from 'node:fs';
import { access, mkdir, readFile, unlink, writeFile } from 'node:fs/promises';
import { resolve, sep } from 'node:path';
import { gzip, gunzip } from 'node:zlib';
import { promisify } from 'node:util';

import { z } from 'zod';

const gzipAsync = promisify(gzip);
const gunzipAsync = promisify(gunzip);

export class ReplayStore {
  public constructor(private readonly directory: string) {}

  public async initialize(): Promise<void> {
    await mkdir(this.directory, { recursive: true });
    await access(this.directory, constants.R_OK | constants.W_OK);
  }

  public async write(fileName: string, events: readonly unknown[]): Promise<{
    byteSize: number;
    rawBytes: number;
  }> {
    const raw = Buffer.from(JSON.stringify(events));
    const compressed = await gzipAsync(raw, { level: 6 });
    await writeFile(this.resolveFile(fileName), compressed, { flag: 'wx', mode: 0o600 });
    return { byteSize: compressed.byteLength, rawBytes: raw.byteLength };
  }

  public async read(fileName: string): Promise<unknown[]> {
    const compressed = await readFile(this.resolveFile(fileName));
    const decoded: unknown = JSON.parse((await gunzipAsync(compressed)).toString('utf8'));
    return z.array(z.unknown()).parse(decoded);
  }

  public async remove(fileName: string): Promise<void> {
    try {
      await unlink(this.resolveFile(fileName));
    } catch (error: unknown) {
      if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) {
        throw error;
      }
    }
  }

  private resolveFile(fileName: string): string {
    if (!/^[A-Za-z0-9._-]+\.json\.gz$/.test(fileName)) {
      throw new Error('Nombre de replay inválido');
    }

    const path = resolve(this.directory, fileName);
    const root = resolve(this.directory) + sep;
    if (!path.startsWith(root)) throw new Error('Ruta de replay inválida');
    return path;
  }
}
