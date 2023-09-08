%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_enterprise).

-if(?EMQX_RELEASE_EDITION == ee).

-include_lib("hocon/include/hoconsc.hrl").
-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    fields/1
]).

fields(connectors) ->
    kafka_structs().

kafka_structs() ->
    [
        {kafka,
            mk(
                hoconsc:map(name, ref(emqx_bridge_kafka, connector_config)),
                #{
                    desc => <<"Kafka Connector Config">>,
                    required => false
                }
            )}
    ].

-else.

-endif.
