import { NetworkConditionLink } from 'nengi';
import type { BinaryAdapter, ClientTransportHandlers, IClientNetworkAdapter, NetworkConditions, NetworkConditionStatus } from 'nengi';
import WebSocket from 'ws';
export type WsClientAdapterStats = {
    /** Binary transport messages, including the connection handshake. */
    messagesReceived: number;
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
    readonly clientAdapterVersion: 2;
    socket: WebSocket | null;
    binary: BinaryAdapter<Buffer>;
    protected link?: NetworkConditionLink;
    private handlers;
    stats: WsClientAdapterStats;
    constructor(config?: WsClientAdapterConfig);
    open(url: string, handlers: ClientTransportHandlers<Buffer>): void;
    send(payload: Buffer): void;
    close(reason: string, force: boolean): void;
}
declare class SimulatedWsClientAdapter extends WsClientAdapter {
    readonly conditions: NetworkConditionLink;
    constructor(config: SimulatedWsClientAdapterConfig);
    configureNetworkConditions(conditions: NetworkConditions): void;
    getNetworkConditionStatus(): NetworkConditionStatus;
}
export { WsClientAdapter, SimulatedWsClientAdapter };
