package main;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Class representing the entry point into the server side application.
 */
public class ServerApp {

    /**
     * Creates a broker and a registry. It then binds the Broker to that registry.
     * @param args
     */
    public static void main(String[] args) {

        try {
            Broker broker = new Broker();
            Registry registry = LocateRegistry.createRegistry(1099);
            registry.bind("broker", broker); // Bind object to the registry to inform others
            System.out.println("Server bereit!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}