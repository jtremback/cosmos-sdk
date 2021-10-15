package keeper

import (
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/address"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
)

// bech32Codec is a address.Codec based on bech32 encoding.
type bech32Codec struct {
	bech32Prefix string
}

var _ address.Codec = &bech32Codec{}

// newBech32Codec creates a new address.Codec based on bech32 encoding.
func newBech32Codec(prefix string) address.Codec {
	return bech32Codec{prefix}
}

// StringToBytes encodes text to bytes.
func (bc bech32Codec) StringToBytes(text string) ([]byte, error) {
	hrp, bz, err := bech32.DecodeAndConvert(text)
	if err != nil {
		return nil, err
	}

	if hrp != bc.bech32Prefix {
		return nil, sdkerrors.Wrap(sdkerrors.ErrLogic, "hrp does not match bech32Prefix")
	}

	if err := sdk.VerifyAddressFormat(bz); err != nil {
		return nil, err
	}

	return bz, nil
}

// BytesToString decodes bytes to text.
func (bc bech32Codec) BytesToString(bz []byte) (string, error) {
	text, err := bech32.ConvertAndEncode(bc.bech32Prefix, bz)
	if err != nil {
		return "", err
	}

	return text, nil
}
