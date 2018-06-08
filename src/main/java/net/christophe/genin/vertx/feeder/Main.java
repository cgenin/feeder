package net.christophe.genin.vertx.feeder;


import io.vertx.core.Launcher;

public class Main extends Launcher {

    public static void main(String[] args) {
        new Main().dispatch(
                args
        );
    }
}
