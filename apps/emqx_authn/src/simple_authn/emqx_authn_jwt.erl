%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_jwt).

-include("emqx_authn.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    refs/0,
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "authn-jwt".

roots() ->
    [
        {?CONF_NS,
            hoconsc:mk(
                hoconsc:union(refs()),
                #{}
            )}
    ].

fields('hmac-based') ->
    [
        {use_jwks, sc(hoconsc:enum([false]), #{required => true, desc => ""})},
        {algorithm,
            sc(hoconsc:enum(['hmac-based']), #{required => true, desc => "Signing algorithm."})},
        {secret, fun secret/1},
        {secret_base64_encoded, fun secret_base64_encoded/1}
    ] ++ common_fields();
fields('public-key') ->
    [
        {use_jwks, sc(hoconsc:enum([false]), #{required => true, desc => ""})},
        {algorithm,
            sc(hoconsc:enum(['public-key']), #{required => true, desc => "Signing algorithm."})},
        {certificate, fun certificate/1}
    ] ++ common_fields();
fields('jwks') ->
    [
        {use_jwks, sc(hoconsc:enum([true]), #{required => true, desc => ""})},
        {endpoint, fun endpoint/1},
        {pool_size, fun pool_size/1},
        {refresh_interval, fun refresh_interval/1},
        {ssl, #{
            type => hoconsc:union([
                hoconsc:ref(?MODULE, ssl_enable),
                hoconsc:ref(?MODULE, ssl_disable)
            ]),
            desc => "Enable/disable SSL.",
            default => #{<<"enable">> => false},
            required => false
        }}
    ] ++ common_fields();
fields(ssl_enable) ->
    [
        {enable, #{type => true, desc => ""}},
        {cacertfile, fun cacertfile/1},
        {certfile, fun certfile/1},
        {keyfile, fun keyfile/1},
        {verify, fun verify/1},
        {server_name_indication, fun server_name_indication/1}
    ];
fields(ssl_disable) ->
    [{enable, #{type => false, desc => ""}}].

desc('hmac-based') ->
    "Settings for HMAC-based token signing algorithm.";
desc('public-key') ->
    "Settings for public key-based token signing algorithm.";
desc('jwks') ->
    "Settings for a signing using JSON Web Key Set (JWKs).";
desc(ssl_disable) ->
    "";
desc(ssl_enable) ->
    "SSL configuration.";
desc(_) ->
    undefined.

common_fields() ->
    [
        {mechanism, emqx_authn_schema:mechanism('jwt')},
        {verify_claims, fun verify_claims/1}
    ] ++ emqx_authn_schema:common_fields().

secret(type) -> binary();
secret(desc) -> "The key to verify the JWT Token using HMAC algorithm.";
secret(required) -> true;
secret(_) -> undefined.

secret_base64_encoded(type) -> boolean();
secret_base64_encoded(desc) -> "Enable/disable base64 encoding of the secret.";
secret_base64_encoded(default) -> false;
secret_base64_encoded(_) -> undefined.

certificate(type) -> string();
certificate(desc) -> "The certificate used for signing the token.";
certificate(required) -> ture;
certificate(_) -> undefined.

endpoint(type) -> string();
endpoint(desc) -> "JWKs endpoint.";
endpoint(required) -> true;
endpoint(_) -> undefined.

refresh_interval(type) -> integer();
refresh_interval(desc) -> "JWKs refresh interval";
refresh_interval(default) -> 300;
refresh_interval(validator) -> [fun(I) -> I > 0 end];
refresh_interval(_) -> undefined.

cacertfile(type) -> string();
cacertfile(desc) -> "Path to the SSL CA certificate file.";
cacertfile(_) -> undefined.

certfile(type) -> string();
certfile(desc) -> "Path to the SSL certificate file.";
certfile(_) -> undefined.

keyfile(type) -> string();
keyfile(desc) -> "Path to the SSL secret key file.";
keyfile(_) -> undefined.

verify(type) -> hoconsc:enum([verify_peer, verify_none]);
verify(desc) -> "Enable or disable SSL peer verification.";
verify(default) -> verify_none;
verify(_) -> undefined.

server_name_indication(type) -> string();
server_name_indication(desc) -> "SSL SNI (Server Name Indication)";
server_name_indication(_) -> undefined.

verify_claims(type) ->
    list();
verify_claims(desc) ->
    "The list of claims to verify.";
verify_claims(default) ->
    #{};
verify_claims(validator) ->
    [fun do_check_verify_claims/1];
verify_claims(converter) ->
    fun(VerifyClaims) ->
        [{to_binary(K), V} || {K, V} <- maps:to_list(VerifyClaims)]
    end;
verify_claims(required) ->
    false;
verify_claims(_) ->
    undefined.

pool_size(type) -> integer();
pool_size(desc) -> "JWKS connection count";
pool_size(default) -> 8;
pool_size(validator) -> [fun(I) -> I > 0 end];
pool_size(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [
        hoconsc:ref(?MODULE, 'hmac-based'),
        hoconsc:ref(?MODULE, 'public-key'),
        hoconsc:ref(?MODULE, 'jwks')
    ].

create(_AuthenticatorID, Config) ->
    create(Config).

create(#{verify_claims := VerifyClaims} = Config) ->
    create2(Config#{verify_claims => handle_verify_claims(VerifyClaims)}).

update(
    #{use_jwks := false} = Config,
    #{jwk_resource := ResourceId}
) ->
    _ = emqx_resource:remove_local(ResourceId),
    create(Config);
update(#{use_jwks := false} = Config, _State) ->
    create(Config);
update(
    #{use_jwks := true} = Config,
    #{jwk_resource := ResourceId} = State
) ->
    case emqx_resource:query(ResourceId, {update, connector_opts(Config)}) of
        ok ->
            case maps:get(verify_claims, Config, undefined) of
                undefined ->
                    {ok, State};
                VerifyClaims ->
                    {ok, State#{verify_claims => handle_verify_claims(VerifyClaims)}}
            end;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "jwks_client_option_update_failed",
                resource => ResourceId,
                reason => Reason
            })
    end;
update(#{use_jwks := true} = Config, _State) ->
    create(Config).

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(
    Credential = #{password := JWT},
    #{
        verify_claims := VerifyClaims0,
        jwk := JWK
    }
) ->
    JWKs = [JWK],
    VerifyClaims = replace_placeholder(VerifyClaims0, Credential),
    verify(JWT, JWKs, VerifyClaims);
authenticate(
    Credential = #{password := JWT},
    #{
        verify_claims := VerifyClaims0,
        jwk_resource := ResourceId
    }
) ->
    case emqx_resource:query(ResourceId, get_jwks) of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "get_jwks_failed",
                resource => ResourceId,
                reason => Reason
            }),
            ignore;
        {ok, JWKs} ->
            VerifyClaims = replace_placeholder(VerifyClaims0, Credential),
            verify(JWT, JWKs, VerifyClaims)
    end.

destroy(#{jwk_resource := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok;
destroy(_) ->
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

create2(#{
    use_jwks := false,
    algorithm := 'hmac-based',
    secret := Secret0,
    secret_base64_encoded := Base64Encoded,
    verify_claims := VerifyClaims
}) ->
    case may_decode_secret(Base64Encoded, Secret0) of
        {error, Reason} ->
            {error, Reason};
        Secret ->
            JWK = jose_jwk:from_oct(Secret),
            {ok, #{
                jwk => JWK,
                verify_claims => VerifyClaims
            }}
    end;
create2(#{
    use_jwks := false,
    algorithm := 'public-key',
    certificate := Certificate,
    verify_claims := VerifyClaims
}) ->
    JWK = create_jwk_from_pem_or_file(Certificate),
    {ok, #{
        jwk => JWK,
        verify_claims => VerifyClaims
    }};
create2(
    #{
        use_jwks := true,
        verify_claims := VerifyClaims
    } = Config
) ->
    ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
    {ok, _} = emqx_resource:create_local(
        ResourceId,
        ?RESOURCE_GROUP,
        emqx_authn_jwks_connector,
        connector_opts(Config)
    ),
    {ok, #{
        jwk_resource => ResourceId,
        verify_claims => VerifyClaims
    }}.

create_jwk_from_pem_or_file(CertfileOrFilePath) when
    is_binary(CertfileOrFilePath);
    is_list(CertfileOrFilePath)
->
    case filelib:is_file(CertfileOrFilePath) of
        true ->
            jose_jwk:from_pem_file(CertfileOrFilePath);
        false ->
            jose_jwk:from_pem(iolist_to_binary(CertfileOrFilePath))
    end.

connector_opts(#{ssl := #{enable := Enable} = SSL} = Config) ->
    SSLOpts =
        case Enable of
            true -> maps:without([enable], SSL);
            false -> #{}
        end,
    Config#{ssl_opts => SSLOpts}.

may_decode_secret(false, Secret) ->
    Secret;
may_decode_secret(true, Secret) ->
    try
        base64:decode(Secret)
    catch
        error:_ ->
            {error, {invalid_parameter, secret}}
    end.

replace_placeholder(L, Variables) ->
    replace_placeholder(L, Variables, []).

replace_placeholder([], _Variables, Acc) ->
    Acc;
replace_placeholder([{Name, {placeholder, PL}} | More], Variables, Acc) ->
    Value = maps:get(PL, Variables),
    replace_placeholder(More, Variables, [{Name, Value} | Acc]);
replace_placeholder([{Name, Value} | More], Variables, Acc) ->
    replace_placeholder(More, Variables, [{Name, Value} | Acc]).

verify(JWT, JWKs, VerifyClaims) ->
    case do_verify(JWT, JWKs, VerifyClaims) of
        {ok, Extra} -> {ok, Extra};
        {error, {missing_claim, _}} -> {error, bad_username_or_password};
        {error, invalid_signature} -> ignore;
        {error, {claims, _}} -> {error, bad_username_or_password}
    end.

do_verify(_JWS, [], _VerifyClaims) ->
    {error, invalid_signature};
do_verify(JWS, [JWK | More], VerifyClaims) ->
    try jose_jws:verify(JWK, JWS) of
        {true, Payload, _JWS} ->
            Claims = emqx_json:decode(Payload, [return_maps]),
            case verify_claims(Claims, VerifyClaims) of
                ok ->
                    {ok, emqx_authn_utils:is_superuser(Claims)};
                {error, Reason} ->
                    {error, Reason}
            end;
        {false, _, _} ->
            do_verify(JWS, More, VerifyClaims)
    catch
        _:_Reason ->
            ?TRACE("JWT", "authn_jwt_invalid_signature", #{jwk => JWK, jws => JWS}),
            {error, invalid_signature}
    end.

verify_claims(Claims, VerifyClaims0) ->
    Now = os:system_time(seconds),
    VerifyClaims =
        [
            {<<"exp">>, fun(ExpireTime) ->
                Now < ExpireTime
            end},
            {<<"iat">>, fun(IssueAt) ->
                IssueAt =< Now
            end},
            {<<"nbf">>, fun(NotBefore) ->
                NotBefore =< Now
            end}
        ] ++ VerifyClaims0,
    do_verify_claims(Claims, VerifyClaims).

do_verify_claims(_Claims, []) ->
    ok;
do_verify_claims(Claims, [{Name, Fun} | More]) when is_function(Fun) ->
    case maps:take(Name, Claims) of
        error ->
            do_verify_claims(Claims, More);
        {Value, NClaims} ->
            case Fun(Value) of
                true ->
                    do_verify_claims(NClaims, More);
                _ ->
                    {error, {claims, {Name, Value}}}
            end
    end;
do_verify_claims(Claims, [{Name, Value} | More]) ->
    case maps:take(Name, Claims) of
        error ->
            {error, {missing_claim, Name}};
        {Value, NClaims} ->
            do_verify_claims(NClaims, More);
        {Value0, _} ->
            {error, {claims, {Name, Value0}}}
    end.

do_check_verify_claims([]) ->
    true;
do_check_verify_claims([{Name, Expected} | More]) ->
    check_claim_name(Name) andalso
        check_claim_expected(Expected) andalso
        do_check_verify_claims(More).

check_claim_name(exp) ->
    false;
check_claim_name(iat) ->
    false;
check_claim_name(nbf) ->
    false;
check_claim_name(_) ->
    true.

check_claim_expected(Expected) ->
    try handle_placeholder(Expected) of
        _ -> true
    catch
        _:_ ->
            false
    end.

handle_verify_claims(VerifyClaims) ->
    handle_verify_claims(VerifyClaims, []).

handle_verify_claims([], Acc) ->
    Acc;
handle_verify_claims([{Name, Expected0} | More], Acc) ->
    Expected = handle_placeholder(Expected0),
    handle_verify_claims(More, [{Name, Expected} | Acc]).

handle_placeholder(Placeholder0) ->
    case re:run(Placeholder0, "^\\$\\{[a-z0-9\\-]+\\}$", [{capture, all}]) of
        {match, [{Offset, Length}]} ->
            Placeholder1 = binary:part(Placeholder0, Offset + 2, Length - 3),
            Placeholder2 = validate_placeholder(Placeholder1),
            {placeholder, Placeholder2};
        nomatch ->
            Placeholder0
    end.

validate_placeholder(<<"clientid">>) ->
    clientid;
validate_placeholder(<<"username">>) ->
    username.

to_binary(A) when is_atom(A) ->
    atom_to_binary(A);
to_binary(B) when is_binary(B) ->
    B.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
