%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_license).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(emqx_config_handler).

-export([pre_config_update/3,
         post_config_update/5
        ]).

-export([load/0,
         check/2,
         unload/0,
         read_license/0,
         update_file/1,
         update_value/1]).

-define(CONF_KEY_PATH, [license]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec read_license() -> {ok, emqx_license_parser:license()} | {error, term()}.
read_license() ->
    read_license(emqx:get_config(?CONF_KEY_PATH)).

-spec load() -> ok.
load() ->
    emqx_license_cli:load(),
    emqx_conf:add_handler(?CONF_KEY_PATH, ?MODULE),
    add_license_hook().

-spec unload() -> ok.
unload() ->
    %% Delete the hook. This means that if the user calls
    %% `application:stop(emqx_license).` from the shell, then here should no limitations!
    del_license_hook(),
    emqx_conf:remove_handler(?CONF_KEY_PATH),
    emqx_license_cli:unload().

-spec update_file(binary() | string()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update_file(Filename) when is_binary(Filename); is_list(Filename) ->
    Result = emqx_conf:update(
               ?CONF_KEY_PATH,
               {file, Filename},
               #{rawconf_with_defaults => true, override_to => cluster}),
    handle_config_update_result(Result).

-spec update_value(binary() | string()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update_value(Value) when is_binary(Value); is_list(Value) ->
    Result = emqx_conf:update(
               ?CONF_KEY_PATH,
               {value, Value},
               #{rawconf_with_defaults => true, override_to => cluster}),
    handle_config_update_result(Result).

%%------------------------------------------------------------------------------
%% emqx_hooks
%%------------------------------------------------------------------------------

check(_ConnInfo, AckProps) ->
    #{max_connections := MaxClients} = emqx_license_checker:limits(),
    case MaxClients of
        0 ->
            ?SLOG(error, #{msg => "Connection rejected due to the license expiration"}),
            {stop, {error, ?RC_QUOTA_EXCEEDED}};
        _ ->
            case check_max_clients_exceeded(MaxClients) of
                true ->
                    ?SLOG(error, #{msg => "Connection rejected due to max clients limitation"}),
                    {stop, {error, ?RC_QUOTA_EXCEEDED}};
                false ->
                    {ok, AckProps}
            end
    end.

%%------------------------------------------------------------------------------
%% emqx_config_handler callbacks
%%------------------------------------------------------------------------------

pre_config_update(_, Cmd, Conf) ->
    {ok, do_update(Cmd, Conf)}.

post_config_update(_Path, _Cmd, NewConf, _Old, _AppEnvs) ->
    case read_license(NewConf) of
        {ok, License} ->
            {ok, emqx_license_checker:update(License)};
        {error, _} = Error -> Error
    end.

%%------------------------------------------------------------------------------
%% Private functions
%%------------------------------------------------------------------------------

add_license_hook() ->
    ok = emqx_hooks:put('client.connect', {?MODULE, check, []}).

del_license_hook() ->
    _ = emqx_hooks:del('client.connect', {?MODULE, check, []}),
    ok.

do_update({file, Filename}, Conf) ->
    case file:read_file(Filename) of
        {ok, Content} ->
            case emqx_license_parser:parse(Content) of
                {ok, _License} ->
                    Conf#{
                      <<"type">> => <<"file">>,
                      <<"value">> => Filename
                     };
                {error, Reason} ->
                    error(Reason)
            end;
        {error, Reason} ->
            error({invalid_license_file, Reason})
    end;

do_update({value, Content}, Conf) when is_binary(Content); is_list(Content) ->
    case emqx_license_parser:parse(Content) of
        {ok, _License} ->
            Conf#{
              <<"type">> => <<"string">>,
              <<"value">> => Content
             };
        {error, Reason} ->
            error(Reason)
    end.

check_max_clients_exceeded(MaxClients) ->
    emqx_license_resources:connection_count() > MaxClients * 1.1.

read_license(#{type := file, value := Filename}) ->
    case file:read_file(Filename) of
        {ok, Content} -> emqx_license_parser:parse(Content);
        {error, _} = Error -> Error
    end;

read_license(#{type := string, value := Content}) ->
    emqx_license_parser:parse(Content).

handle_config_update_result({error, _} = Error) -> Error;
handle_config_update_result({ok, #{post_config_update := #{emqx_license := Result}}}) -> {ok, Result}.
