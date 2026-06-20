# Game Compressor - standalone PS5 payload build.

SHELL := bash

ifeq ($(strip $(PS5_PAYLOAD_SDK)),)
$(error PS5_PAYLOAD_SDK is required, e.g. export PS5_PAYLOAD_SDK=/path/to/ps5-payload-sdk)
endif

include $(PS5_PAYLOAD_SDK)/toolchain/prospero.mk

PYTHON ?= python3
LLVM_BINDIR ?= $(shell dirname "$$(command -v clang 2>/dev/null || command -v llvm-strip 2>/dev/null || echo clang)" 2>/dev/null || echo .)
LLVM_CONFIG ?= $(CURDIR)/build-tools/llvm-config
export LLVM_BINDIR
export LLVM_CONFIG

BIN := game-compressor.elf
BUILD_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

C_SRCS := src/gc_main.c
C_SRCS += src/gc_websrv.c
C_SRCS += src/gc_api.c
C_SRCS += src/gc_job.c
C_SRCS += src/gc_diag.c
C_SRCS += src/gc_notify.c
C_SRCS += src/gc_power_guard.c
C_SRCS += src/gc_app_installer.c
C_SRCS += src/gc_shadowmount.c
C_SRCS += src/gc_size_cache.c
C_SRCS += src/gc_icon_thumb.c
C_SRCS += src/ampr_index.c
C_SRCS += src/asset.c
C_SRCS += src/pfs_compress.c
C_SRCS += src/pfs_decompress.c
C_SRCS += src/pfs_repair.c
C_SRCS += src/pfs_ampr_hotswap.c
C_SRCS += src/pfs_block_pipeline.c
C_SRCS += src/pfs_validate_hash.c
C_SRCS += src/miniz_tinfl.c

ASSETS := $(wildcard assets/*)
GEN_SRCS := $(patsubst assets/%,gen/assets/%,$(ASSETS:=.c))
APP_ASSETS := assets-app/param.json assets-app/icon0.png
OBJS := $(patsubst %.c,build/%.o,$(C_SRCS) $(GEN_SRCS))

CFLAGS_COMMON := -Os -Wall -Werror -Isrc
CFLAGS_COMMON += -ffunction-sections -fdata-sections -flto
CFLAGS_COMMON += -DGAME_COMPRESSOR_VERSION=\"$(BUILD_VERSION)\"
CFLAGS_COMMON += -DGAME_COMPRESSOR_PORT=5910
CFLAGS_COMMON += -DVERSION_TAG=\"game-compressor\"
CFLAGS_COMMON += -DBUILD_VERSION=\"$(BUILD_VERSION)\"
CFLAGS_COMMON += -DMINIZ_USE_UNALIGNED_LOADS_AND_STORES=1

FAST_SRCS := src/pfs_compress.c src/pfs_decompress.c src/pfs_repair.c src/pfs_ampr_hotswap.c src/pfs_block_pipeline.c src/pfs_validate_hash.c src/miniz_tinfl.c
FAST_OBJS := $(patsubst %.c,build/%.o,$(FAST_SRCS))

PFSC_ENCODER ?= runtime
PFSC_ZLIB_LEVEL ?= 7
PFSC_THRESHOLD_GAIN ?= 5
PFSC_FORCE_RAW_EXEC ?= 1
ZLIB_INCLUDE ?= /Users/jumasayeh/Developer/etaHEN/Source\ Code/include
ZLIB_LIB ?= /Users/jumasayeh/Developer/etaHEN/Source\ Code/lib/libz.a

ifneq ($(filter $(PFSC_ENCODER),runtime zlib miniz),)
CFLAGS_COMMON += -DGC_PFSC_ZLIB_LEVEL=$(PFSC_ZLIB_LEVEL)
CFLAGS_COMMON += -DGC_PFSC_THRESHOLD_GAIN=$(PFSC_THRESHOLD_GAIN)
CFLAGS_COMMON += -DGC_PFSC_FORCE_RAW_EXEC=$(PFSC_FORCE_RAW_EXEC)
CFLAGS_COMMON += -I$(ZLIB_INCLUDE)
else
$(error unsupported PFSC_ENCODER=$(PFSC_ENCODER), expected runtime, zlib, or miniz)
endif

FAST_CFLAGS := $(filter-out -Os,$(CFLAGS_COMMON)) -O2
LDFLAGS_COMMON := -Wl,--gc-sections -flto
LDADD := -lSceNotification -lSceSystemService
LDADD += $(ZLIB_LIB)

all: $(BIN)

gen/assets:
	mkdir -p $@

gen/assets/%.c: assets/% | gen/assets
	$(PYTHON) gen-asset-module.py --path $* $< > $@

$(FAST_OBJS): build/%.o: %.c Makefile
	mkdir -p $(dir $@)
	"$(CC)" $(FAST_CFLAGS) -c $< -o $@

build/src/gc_app_installer.o: $(APP_ASSETS)

build/%.o: %.c Makefile
	mkdir -p $(dir $@)
	"$(CC)" $(CFLAGS_COMMON) -c $< -o $@

$(BIN): $(OBJS) $(APP_ASSETS)
	"$(CC)" $(CFLAGS_COMMON) $(LDFLAGS_COMMON) -o $@ $(OBJS) $(LDADD)
	"$(STRIP)" --strip-all $@

clean:
	rm -rf build gen $(BIN)

.SECONDARY: $(GEN_SRCS)
.PHONY: all clean
