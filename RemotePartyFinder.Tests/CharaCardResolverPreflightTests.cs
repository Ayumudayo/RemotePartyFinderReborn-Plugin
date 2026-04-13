using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class CharaCardResolverPreflightTests {
    [Fact]
    public void ResolverPreflight_returns_disabled_when_interop_addresses_are_unavailable() {
        var result = ResolverPreflightEvaluator.Evaluate(
            requestCharaCardAddress: 0,
            handleCurrentCharaCardDataPacketAddress: 0x1400,
            openCharaCardForPacketAddress: 0x1500
        );

        Assert.False(result.Enabled);
        Assert.Contains("RequestCharaCardForContentId", result.Reason);
    }

    [Fact]
    public void ResolverPreflight_returns_disabled_when_handle_current_chara_card_data_packet_address_is_unavailable() {
        var result = ResolverPreflightEvaluator.Evaluate(
            requestCharaCardAddress: 0x1400,
            handleCurrentCharaCardDataPacketAddress: 0,
            openCharaCardForPacketAddress: 0x1500
        );

        Assert.False(result.Enabled);
        Assert.Equal("HandleCurrentCharaCardDataPacket interop address is unavailable.", result.Reason);
    }

    [Fact]
    public void ResolverPreflight_returns_disabled_when_open_chara_card_for_packet_address_is_unavailable() {
        var result = ResolverPreflightEvaluator.Evaluate(
            requestCharaCardAddress: 0x1400,
            handleCurrentCharaCardDataPacketAddress: 0x1500,
            openCharaCardForPacketAddress: 0
        );

        Assert.False(result.Enabled);
        Assert.Equal("OpenCharaCardForPacket interop address is unavailable.", result.Reason);
    }
}
