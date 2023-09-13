%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_connector_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([transform_connector_less_bridges/1]).

-export([roots/0, fields/1, desc/1, namespace/0, tags/0]).

-if(?EMQX_RELEASE_EDITION == ee).

enterprise_fields_connectors() ->
    %% We *must* do this to ensure the module is really loaded, especially when we use
    %% `call_hocon' from `nodetool' to generate initial configurations.
    _ = emqx_connector_enterprise:module_info(),
    case erlang:function_exported(emqx_connector_enterprise, fields, 1) of
        true ->
            emqx_connector_enterprise:fields(connectors);
        false ->
            []
    end.

-else.

enterprise_fields_connectors() -> [].

-endif.

connector_type_to_bridge_types(kafka) -> [kafka].

has_connector_field(BridgeConf, ConnectorFields) ->
    lists:any(
        fun({ConnectorFieldName, _Spec}) ->
            maps:is_key(to_bin(ConnectorFieldName), BridgeConf)
        end,
        ConnectorFields
    ).

bridge_configs_to_transform(_BridgeType, [] = _BridgeNameBridgeConfList, _ConnectorFields) ->
    [];
bridge_configs_to_transform(BridgeType, [{BridgeName, BridgeConf} | Rest], ConnectorFields) ->
    case has_connector_field(BridgeConf, ConnectorFields) of
        true ->
            [
                {BridgeType, BridgeName, BridgeConf, ConnectorFields}
                | bridge_configs_to_transform(BridgeType, Rest, ConnectorFields)
            ];
        false ->
            bridge_configs_to_transform(BridgeType, Rest, ConnectorFields)
    end.

split_bridge_to_connector_and_bridge(
    {ConnectorsMap, {BridgeType, BridgeName, BridgeConf, ConnectorFields}}
) ->
    %% Get connector fields from bridge config
    ConnectorMap = lists:foldl(
        fun({ConnectorFieldName, _Spec}, ToTransformSoFar) ->
            case maps:is_key(to_bin(ConnectorFieldName), BridgeConf) of
                true ->
                    NewToTransform = maps:put(
                        to_bin(ConnectorFieldName),
                        maps:get(to_bin(ConnectorFieldName), BridgeConf),
                        ToTransformSoFar
                    ),
                    NewToTransform;
                false ->
                    ToTransformSoFar
            end
        end,
        #{},
        ConnectorFields
    ),
    %% Remove connector fields from bridge config
    BridgeMap0 = lists:foldl(
        fun({ConnectorFieldName, _Spec}, ToTransformSoFar) ->
            case maps:is_key(to_bin(ConnectorFieldName), BridgeConf) of
                true ->
                    maps:remove(to_bin(ConnectorFieldName), ToTransformSoFar);
                false ->
                    ToTransformSoFar
            end
        end,
        BridgeConf,
        ConnectorFields
    ),
    %% Generate an connector name with BridgeName as prefix and a random suffix until no conflict in ConnectorsMap
    ConnectorName = generate_connector_name(ConnectorsMap, BridgeName),
    %% Add connector field to bridge map
    BridgeMap = maps:put(<<"connector">>, ConnectorName, BridgeMap0),
    {BridgeType, BridgeName, BridgeMap, ConnectorName, ConnectorMap}.

generate_connector_name(ConnectorsMap, BridgeName) ->
    ConnectorNameList = lists:flatten(
        io_lib:format("~s_~p", [BridgeName, rand:uniform(1000000000)])
    ),
    ConnectorName = list_to_binary(ConnectorNameList),
    case maps:is_key(ConnectorName, ConnectorsMap) of
        true ->
            generate_connector_name(ConnectorsMap, BridgeName);
        false ->
            ConnectorName
    end.

transform_connector_less_bridges_of_type(
    {ConnectorType, #{type := {map, name, {ref, ConnectorConfSchemaMod, ConnectorConfSchemaName}}}},
    RawConfig
) ->
    ConnectorFields = ConnectorConfSchemaMod:fields(ConnectorConfSchemaName),
    BridgeTypes = connector_type_to_bridge_types(ConnectorType),
    BridgesConfMap = maps:get(<<"bridges">>, RawConfig, #{}),
    ConnectorsConfMap = maps:get(<<"connectors">>, RawConfig, #{}),
    BridgeConfigsToTransform1 =
        lists:foldl(
            fun(BridgeType, ToTranformSoFar) ->
                BridgeNameToBridgeMap = maps:get(to_bin(BridgeType), BridgesConfMap, #{}),
                BridgeNameBridgeConfList = maps:to_list(BridgeNameToBridgeMap),
                NewToTransform = bridge_configs_to_transform(
                    BridgeType, BridgeNameBridgeConfList, ConnectorFields
                ),
                [NewToTransform, ToTranformSoFar]
            end,
            [],
            BridgeTypes
        ),
    BridgeConfigsToTransform = lists:flatten(BridgeConfigsToTransform1),
    BridgeConfigsToTransformWithConnectorConf = lists:zip(
        lists:duplicate(length(BridgeConfigsToTransform), ConnectorsConfMap),
        BridgeConfigsToTransform
    ),
    BridgeConnectorModifications = lists:map(
        fun split_bridge_to_connector_and_bridge/1,
        BridgeConfigsToTransformWithConnectorConf
    ),
    %% Apply modifications to RawConfig
    lists:foldl(
        fun({BridgeType, BridgeName, BridgeMap, ConnectorName, ConnectorMap}, RawConfigSoFar) ->
            RawConfigSoFar1 = emqx_utils_maps:deep_put(
                [<<"connectors">>, to_bin(ConnectorType), ConnectorName],
                RawConfigSoFar,
                ConnectorMap
            ),
            RawConfigSoFar2 = emqx_utils_maps:deep_remove(
                [<<"bridges">>, to_bin(BridgeType), BridgeName],
                RawConfigSoFar1
            ),
            RawConfigSoFar3 = emqx_utils_maps:deep_put(
                [<<"bridges">>, to_bin(BridgeType), BridgeName],
                RawConfigSoFar2,
                BridgeMap
            ),
            RawConfigSoFar3
        end,
        RawConfig,
        BridgeConnectorModifications
    ).

transform_connector_less_bridges(RawConfig) ->
    ConnectorFields = fields(connectors),
    NewRawConf = lists:foldl(
        fun transform_connector_less_bridges_of_type/2,
        RawConfig,
        ConnectorFields
    ),
    NewRawConf.

%%======================================================================================
%% HOCON Schema Callbacks
%%======================================================================================

namespace() -> "connector".

tags() ->
    [<<"Connector">>].

roots() -> [{connectors, ?HOCON(?R_REF(connectors), #{importance => ?IMPORTANCE_LOW})}].

fields(connectors) ->
    [] ++ enterprise_fields_connectors().

desc(_) ->
    undefined.

%%======================================================================================
%% Helper Functions
%%======================================================================================

to_bin(Atom) when is_atom(Atom) ->
    list_to_binary(atom_to_list(Atom));
to_bin(Bin) when is_binary(Bin) ->
    Bin;
to_bin(Something) ->
    Something.
