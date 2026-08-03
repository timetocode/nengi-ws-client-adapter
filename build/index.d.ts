import { NetworkConditionLink } from 'nengi';
import type { BinaryAdapter, ClientNetwork, IClientNetworkAdapter, NetworkConditions, NetworkConditionStatus } from 'nengi';
import WebSocket from 'ws';
export type WsClientAdapterStats = {
    snapshotsReceived: number;
    bytesReceived: number;
    bytesSent: number;
};
export type WsClientAdapterConfig = {
    binary?: BinaryAdapter<Buffer>;
};
export type SimulatedWsClientAdapterConfig = WsClientAdapterConfig & {
    conditions: NetworkConditions;
};
declare class WsClientAdapter implements IClientNetworkAdapter<Buffer, Buffer, string> {
    socket: WebSocket | null;
    network: ClientNetwork;
    binary: BinaryAdapter<Buffer>;
    connected: boolean;
    stats: WsClientAdapterStats;
    constructor(network: ClientNetwork, config?: WsClientAdapterConfig);
    flush(): void;
    flushPongs(): void;
    disconnect(reason?: any): void;
    private setupWebsocket;
    connect(wsUrl: string, handshake?: any): Promise<unknown>;
}
declare class SimulatedWsClientAdapter implements IClientNetworkAdapter<Buffer, Buffer, string> {
    socket: WebSocket | null;
    network: ClientNetwork;
    binary: BinaryAdapter<Buffer>;
    connected: boolean;
    stats: WsClientAdapterStats;
    readonly conditions: NetworkConditionLink;
    constructor(network: ClientNetwork, config: SimulatedWsClientAdapterConfig);
    flush(): void;
    flushPongs(): void;
    disconnect(reason?: any): void;
    configureNetworkConditions(conditions: NetworkConditions): void;
    getNetworkConditionStatus(): NetworkConditionStatus;
    connect(wsUrl: string, handshake?: any): Promise<unknown>;
    private send;
}
export { WsClientAdapter, SimulatedWsClientAdapter };
