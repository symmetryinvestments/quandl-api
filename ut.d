import unit_threaded;

int main(string[] args) {
    return args.runTests!(
        "kaleidic.api.quandl.index",
        "kaleidic.api.quandl.tickers",
    );
}
