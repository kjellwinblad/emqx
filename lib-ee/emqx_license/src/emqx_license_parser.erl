%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% @doc EMQ X License Management.
%%--------------------------------------------------------------------

-module(emqx_license_parser).

-include_lib("emqx/include/logger.hrl").
-include("emqx_license.hrl").

% -define(PUBKEY, <<"MIIBCgKCAQEAyJgH+BvEJIStYvyw1keQ/ixVPJ4GGjlP7lTKnZL
%                   3mqZyPXQUEaLnRmcQ3/ra8xYQPcfMReynqmrYmT45/eD2tK7rdXT
%                   zYfOWoU0cXNQMaQS7be1bLF4QrAEbJhEsgkjX9rP30mrzZCjRRnk
%                   QtWmi4DNBU4qOl6Ee9aAS5aY+L7DW646J47Tyc7gAA4sdZw04KGB
%                   XnSyXzyBvPaf+QByOrOXUxBcxehHN/Ox41/duYFXSR40U6lyp49N
%                   YJ6yEUVWSp4oxsrkcgqyegNKXdW1D8oqJXzxalbh/RB8YYlX+Ae3
%                   77gEBlLefPFdSEYDRN/ajs3UIeqde6i20lVyDPIjEcQIDAQAB">>).


-define(PUBKEY, <<"MEgCQQChzN6lCUdt4sYPQmWBYA3b8Zk87Jfk+1A1zcTd+lCU0Tf
                  vXhSHgEWz18No4lL2v1n+70CoYpc2fzfhNJitgnV9AgMBAAE=">>).

-define(LICENSE_PARSE_MODULES, [emqx_license_parser_v20220101
                               ]).

-type license_data() :: term().
-type customer_type() :: ?SMALL_CUSTOMER |
                         ?MEDIUM_CUSTOMER |
                         ?LARGE_CUSTOMER |
                         ?EVALUATION_CUSTOMER.

-type license_type() :: ?OFFICIAL | ?TRIAL.

-type license() :: #{module := module(), data := license_data()}.

-export_type([license_data/0,
              customer_type/0,
              license_type/0,
              license/0]).


-export([parse/1,
         parse/2,
         dump/1,
         customer_type/1,
         license_type/1,
         expiry_date/1,
         max_connections/1
        ]).

-ifdef(TEST).
-export([public_key/0
        ]).
-endif.

%%--------------------------------------------------------------------
%% Behaviour
%%--------------------------------------------------------------------

-callback parse(string() | binary(), binary()) -> {ok, license_data()} | {error, term()}.

-callback dump(license_data()) -> list({atom(), term()}).

-callback customer_type(license_data()) -> customer_type().

-callback license_type(license_data()) -> license_type().

-callback expiry_date(license_data()) -> calendar:date().

-callback max_connections(license_data()) -> non_neg_integer().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec parse(string() | binary()) -> {ok, license()} | {error, term()}.
parse(Content) ->
    DecodedKey = base64:decode(public_key()),
    parse(Content, DecodedKey).

parse(Content, Key) ->
    do_parse(iolist_to_binary(Content), Key, ?LICENSE_PARSE_MODULES, []).

-spec dump(license()) -> list({atom(), term()}).
dump(#{module := Module, data := LicenseData}) ->
    Module:dump(LicenseData).

-spec customer_type(license()) -> customer_type().
customer_type(#{module := Module, data := LicenseData}) ->
    Module:customer_type(LicenseData).

-spec license_type(license()) -> license_type().
license_type(#{module := Module, data := LicenseData}) ->
    Module:license_type(LicenseData).

-spec expiry_date(license()) -> calendar:date().
expiry_date(#{module := Module, data := LicenseData}) ->
    Module:expiry_date(LicenseData).

-spec max_connections(license()) -> non_neg_integer().
max_connections(#{module := Module, data := LicenseData}) ->
    Module:max_connections(LicenseData).

%%--------------------------------------------------------------------
%% Private functions
%%--------------------------------------------------------------------

do_parse(_Content, _Key, [], Errors) ->
    {error, {unknown_format, lists:reverse(Errors)}};

do_parse(Content, Key, [Module | Modules], Errors) ->
    try Module:parse(Content, Key) of
        {ok, LicenseData} ->
            {ok, #{module => Module, data => LicenseData}};
        {error, Error} ->
            do_parse(Content, Key, Modules, [{Module, Error} | Errors])
    catch
        _Class:Error:_Stk ->
            do_parse(Content, Key, Modules, [{Module, Error} | Errors])
    end.

public_key() -> ?PUBKEY.
