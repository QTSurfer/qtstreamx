package com.qtsurfer.qtstreamx.dex.discovery.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

/** Renders stable JSON or concise terminal text from the same response model. */
final class CliRenderer {
    private final ObjectMapper objectMapper = new ObjectMapper();

    String render(CliResponse response, CliRequest.Output output) {
        return output == CliRequest.Output.JSON ? json(response) : human(response);
    }

    private String json(CliResponse response) {
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response) + "\n";
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("CLI response serialization failed", exception);
        }
    }

    private String human(CliResponse response) {
        StringBuilder output = new StringBuilder();
        response.messages().forEach(message -> output.append(message).append('\n'));
        if (response.data() instanceof CliData.TokenLookup lookup) {
            appendToken(output, lookup.token());
            lookup.pools().forEach(pool -> appendPool(output, pool));
        } else if (response.data() instanceof List<?> values) {
            values.forEach(value -> appendValue(output, value));
        } else {
            output.append(response.data()).append('\n');
        }
        return output.toString();
    }

    private static void appendValue(StringBuilder output, Object value) {
        switch (value) {
            case CliData.Protocol protocol -> output
                    .append(protocol.alias()).append("  ")
                    .append(protocol.label()).append("  versions=")
                    .append(String.join(",", protocol.versions())).append('\n');
            case CliData.Network network -> output
                    .append(network.alias()).append("  ")
                    .append(network.network()).append("  ")
                    .append(network.label()).append('\n');
            case CliData.Market market -> output
                    .append(market.version()).append("  ")
                    .append(market.instrument()).append("  ")
                    .append(market.address()).append("  reviewed=")
                    .append(market.reviewed()).append('\n');
            case CliData.Pool pool -> appendPool(output, pool);
            case CliData.FactoryMarket market -> output
                    .append(market.version()).append("  block=")
                    .append(market.blockNumber()).append("  ")
                    .append(market.address()).append("  ")
                    .append(market.token0Address()).append("/")
                    .append(market.token1Address()).append("  reviewed=")
                    .append(market.reviewed()).append('\n');
            case CliData.SearchMatch match -> {
                appendToken(output, match.token());
                appendValue(output, match.market());
            }
            case String text -> output.append(text).append('\n');
            default -> throw new IllegalStateException("unsupported CLI response data");
        }
    }

    private static void appendToken(StringBuilder output, CliData.Token token) {
        output.append("token ").append(token.address())
                .append(" name=").append(value(token.name()))
                .append(" symbol=").append(value(token.symbol()))
                .append(" decimals=").append(value(token.decimals()))
                .append(" trusted=false\n");
    }

    private static void appendPool(StringBuilder output, CliData.Pool pool) {
        output.append(pool.version()).append(" pool ").append(pool.address())
                .append(" factory=").append(pool.factoryAddress())
                .append(" tokens=").append(pool.token0().address())
                .append("/").append(pool.token1().address())
                .append(" instrument=").append(value(pool.instrument()))
                .append(" orientation=").append(pool.orientation())
                .append(" reviewed=").append(pool.reviewed());
        if (pool.feeTier() != null) {
            output.append(" fee=").append(pool.feeTier())
                    .append(" activeLiquidity=").append(pool.currentLiquidity());
        } else {
            output.append(" reserves=").append(pool.reserve0())
                    .append("/").append(pool.reserve1());
        }
        output.append('\n');
    }

    private static String value(Object value) {
        return value == null ? "unavailable" : value.toString();
    }
}
