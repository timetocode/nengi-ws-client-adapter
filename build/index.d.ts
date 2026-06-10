import { ClientNetwork } from 'nengi';
import type { BinaryAdapter, IClientNetworkAdapter } from 'nengi';
import WebSocket from 'ws';
export type WsClientAdapterStats = {
    snapshotsReceived: number;
    bytesReceived: number;
    bytesSent: number;
};
declare class WsClientAdapter implements IClientNetworkAdapter<Buffer, Buffer, string> {
    socket: WebSocket | null;
    network: ClientNetwork;
    binary: BinaryAdapter<Buffer>;
    connected: boolean;
    stats: WsClientAdapterStats;
    constructor(network: ClientNetwork, config?: any);
    flush(): void;
    disconnect(code?: number, reason?: string): void;
    private setupWebsocket;
    connect(wsUrl: string, handshake: any): Promise<unknown>;
}
export { WsClientAdapter };
