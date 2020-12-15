REBAR_VERSION = 3.14.3-emqx-3
REBAR = $(CURDIR)/rebar3
BUILD = $(CURDIR)/build
export PKG_VSN ?= $(shell $(CURDIR)/pkg-vsn.sh)

PROFILE ?= emqx
REL_PROFILES := emqx emqx-edge
PKG_PROFILES := emqx-pkg emqx-edge-pkg
PROFILES := $(REL_PROFILES) $(PKG_PROFILES)

export REBAR_GIT_CLONE_OPTIONS += --depth=1

.PHONY: default
default: $(REBAR) $(PROFILE)

.PHONY: all
all: $(REBAR) $(PROFILES)

.PHONY: ensure-rebar3
ensure-rebar3:
	$(CURDIR)/ensure-rebar3.sh $(REBAR_VERSION)

$(REBAR): ensure-rebar3

.PHONY: eunit
eunit: $(REBAR)
	$(REBAR) eunit

.PHONY: ct
ct: $(REBAR)
	$(REBAR) ct

.PHONY: cover
cover: $(REBAR)
	$(REBAR) cover

.PHONY: coveralls
coveralls: $(REBAR)
	$(REBAR) as test coveralls send

.PHONY: $(REL_PROFILES)
$(REL_PROFILES:%=%): $(REBAR)
ifneq ($(shell echo $(@) |grep edge),)
	export EMQX_DESC="EMQ X Edge"
else
	export EMQX_DESC="EMQ X Broker"
endif
	$(REBAR) as $(@) release

# rebar clean
.PHONY: clean $(PROFILES:%=clean-%)
clean: $(PROFILES:%=clean-%)
$(PROFILES:%=clean-%): $(REBAR)
	$(REBAR) as $(@:clean-%=%) clean

.PHONY: deps-all
deps-all: $(REBAR) $(PROFILES:%=deps-%)

.PHONY: $(PROFILES:%=deps-%)
$(PROFILES:%=deps-%): $(REBAR)
ifneq ($(shell echo $(@) |grep edge),)
	export EMQX_DESC="EMQ X Edge"
else
	export EMQX_DESC="EMQ X Broker"
endif
	$(REBAR) as $(@:deps-%=%) get-deps

.PHONY: xref
xref: $(REBAR)
	$(REBAR) as check xref

.PHONY: dialyzer
dialyzer: $(REBAR)
	$(REBAR) as check dialyzer

.PHONY: $(REL_PROFILES:%=relup-%)
$(REL_PROFILES:%=relup-%): $(REBAR)
ifneq ($(OS),Windows_NT)
	$(BUILD) $(@:relup-%=%) relup
endif

.PHONY: $(REL_PROFILES:%=%-tar) $(PKG_PROFILES:%=%-tar)
$(REL_PROFILES:%=%-tar) $(PKG_PROFILES:%=%-tar): $(REBAR)
	$(BUILD) $(subst -tar,,$(@)) tar

## zip targets depend on the corresponding relup and tar artifacts
.PHONY: $(REL_PROFILES:%=%-zip)
define gen-zip-target
$1-zip: relup-$1 $1-tar
	$(BUILD) $1 zip
endef
ALL_ZIPS = $(REL_PROFILES) $(PKG_PROFILES)
$(foreach zt,$(ALL_ZIPS),$(eval $(call gen-zip-target,$(zt))))

## A pkg target depend on a regular release profile zip to include relup,
## and also a package profile tar (without relup) for making deb/rpm package
## TODO: Check if this statement is true.
## QUESTION: Why can't we make deb/rpm packages from regular release profile tar (delete the relup)?
.PHONY: $(PKG_PROFILES)
define gen-pkg-target
$1: $(subst -pkg,,$1)-zip $1-tar
	$(BUILD) $1 pkg
endef
$(foreach pt,$(PKG_PROFILES),$(eval $(call gen-pkg-target,$(pt))))

include docker.mk
