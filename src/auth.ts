import type { FastifyReply, FastifyRequest } from 'fastify';
import { createRemoteJWKSet, jwtVerify } from 'jose';

import type { AppConfig } from './config.js';

export interface AuthContext {
  ownerId: string;
}

export class SupabaseAuth {
  private readonly jwks: ReturnType<typeof createRemoteJWKSet>;

  public constructor(private readonly config: AppConfig) {
    this.jwks = createRemoteJWKSet(
      new URL(`${config.supabaseUrl}/auth/v1/.well-known/jwks.json`),
    );
  }

  public async authenticate(request: FastifyRequest, reply: FastifyReply): Promise<void> {
    const authorization = request.headers.authorization;
    if (!authorization || !/^Bearer\s+/i.test(authorization)) {
      await reply.code(401).send({ error: 'unauthorized' });
      return;
    }

    const token = authorization.replace(/^Bearer\s+/i, '').trim();
    if (!token) {
      await reply.code(401).send({ error: 'unauthorized' });
      return;
    }

    try {
      const { payload } = await jwtVerify(token, this.jwks, {
        algorithms: ['ES256', 'RS256', 'EdDSA'],
        audience: this.config.supabaseAudience,
        issuer: this.config.supabaseIssuer,
      });

      if (
        typeof payload.sub !== 'string' ||
        payload.sub.length === 0 ||
        payload.sub.length > 255 ||
        payload.role !== 'authenticated'
      ) {
        throw new Error('Claims no autorizados');
      }

      request.auth = { ownerId: payload.sub };
      return;
    } catch {
      const apiKey = request.headers.apikey;
      if (typeof apiKey === 'string' && apiKey.length > 0) {
        try {
          const response = await fetch(`${this.config.supabaseUrl}/auth/v1/user`, {
            headers: {
              Authorization: `Bearer ${token}`,
              apikey: apiKey,
            },
          });
          if (response.ok) {
            const user: unknown = await response.json();
            if (user && typeof user === 'object' && 'id' in user) {
              const id = user.id;
              if (typeof id === 'string' && id.length > 0 && id.length <= 255) {
                request.auth = { ownerId: id };
                return;
              }
            }
          }
        } catch {
          // La respuesta 401 común se maneja abajo.
        }
      }
      await reply.code(401).send({ error: 'unauthorized' });
    }
  }
}
