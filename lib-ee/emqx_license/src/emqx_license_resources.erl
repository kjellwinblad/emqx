%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_resources).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

-define(CHECK_INTERVAL, 5000).

-export([start_link/0,
         start_link/1,
         local_connection_count/0,
         connection_count/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link(?CHECK_INTERVAL).

-spec start_link(timeout()) -> {ok, pid()}.
start_link(CheckInterval) when is_integer(CheckInterval) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [CheckInterval], []).

-spec local_connection_count() -> non_neg_integer().
local_connection_count() ->
    emqx_cm:get_connected_client_count().

-spec connection_count() -> non_neg_integer().
connection_count() ->
    local_connection_count() + cached_remote_connection_count().

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([CheckInterval]) ->
    _ = ets:new(?MODULE, [set, protected, named_table]),
    State = ensure_timer(#{check_peer_interval => CheckInterval}),
    {ok, State}.

handle_call(_Req, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(update_resources, State) ->
    true = update_resources(),
    ?tp(debug, emqx_license_resources_updated, #{}),
    {noreply, ensure_timer(State)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Private functions
%%------------------------------------------------------------------------------

cached_remote_connection_count() ->
    try ets:lookup(?MODULE, remote_connection_count) of
        [{remote_connection_count, N}] -> N;
        _ -> 0
    catch
        error:badarg -> 0
    end.

update_resources() ->
    ets:insert(?MODULE, {remote_connection_count, remote_connection_count()}).

ensure_timer(#{check_peer_interval := CheckInterval} = State) ->
    _ = case State of
            #{timer := Timer} -> erlang:cancel_timer(Timer);
            _ -> ok
        end,
    State#{timer => erlang:send_after(CheckInterval, self(), update_resources)}.

remote_connection_count() ->
    Nodes = mria_mnesia:running_nodes() -- [node()],
    Results = emqx_license_proto_v1:remote_connection_counts(Nodes),
    Counts = [Count || {ok, Count} <- Results],
    lists:sum(Counts).
