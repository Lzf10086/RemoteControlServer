import { DurableObject } from "cloudflare:workers";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // 首页测试
    if (url.pathname === "/") {
      return new Response("RemoteControl Server OK");
    }

    // WebSocket连接
    if (url.pathname === "/ws") {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("WebSocket Required", {
          status: 426,
        });
      }

      // 固定使用一个 Durable Object
      const id = env.REMOTE_CONTROL.idFromName("main");
      const stub = env.REMOTE_CONTROL.get(id);

      return stub.fetch(request);
    }

    return new Response("Not Found", {
      status: 404,
    });
  },
};


export class RemoteControlServer extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);

    // 当前连接
    this.connections = new Map();

    // 恢复连接状态
    for (const ws of this.ctx.getWebSockets()) {
      const data = ws.deserializeAttachment();

      if (data) {
        this.connections.set(ws, data);
      }
    }
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("WebSocket Required", {
        status: 426,
      });
    }

    const role = url.searchParams.get("role");

    if (role !== "A" && role !== "B") {
      return new Response(
        "Missing role. Use ?role=A or ?role=B",
        {
          status: 400,
        }
      );
    }

    const webSocketPair = new WebSocketPair();

    const [client, server] = Object.values(webSocketPair);

    // 使用 Durable Object WebSocket Hibernation
    this.ctx.acceptWebSocket(server);

    const connectionInfo = {
      role: role,
      connectedAt: Date.now(),
    };

    server.serializeAttachment(connectionInfo);

    this.connections.set(server, connectionInfo);

    // 通知另一台手机
    this.broadcastStatus();

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  async webSocketMessage(ws, message) {
    const sender = this.connections.get(ws);

    if (!sender) {
      return;
    }

    const text =
      typeof message === "string"
        ? message
        : new TextDecoder().decode(message);

    console.log(
      `收到 ${sender.role} 消息: ${text}`
    );

    // A -> B
    // B -> A
    for (const [otherWs, otherInfo] of this.connections) {
      if (otherWs === ws) {
        continue;
      }

      if (otherWs.readyState === WebSocket.OPEN) {
        try {
          otherWs.send(text);
        } catch (error) {
          console.log(
            "发送失败:",
            error
          );
        }
      }
    }
  }

  async webSocketClose(ws) {
    const info = this.connections.get(ws);

    if (info) {
      console.log(
        `${info.role} 已断开`
      );
    }

    this.connections.delete(ws);

    this.broadcastStatus();
  }

  async webSocketError(ws) {
    const info = this.connections.get(ws);

    if (info) {
      console.log(
        `${info.role} WebSocket错误`
      );
    }

    this.connections.delete(ws);

    this.broadcastStatus();
  }

  broadcastStatus() {
    let hasA = false;
    let hasB = false;

    for (const [ws, info] of this.connections) {
      if (info.role === "A") {
        hasA = true;
      }

      if (info.role === "B") {
        hasB = true;
      }
    }

    const message = JSON.stringify({
      type: "STATUS",
      phoneA: hasA,
      phoneB: hasB,
    });

    for (const ws of this.connections.keys()) {
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(message);
        } catch (error) {
          console.log(
            "状态发送失败:",
            error
          );
        }
      }
    }
  }
}
