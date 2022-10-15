package main;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.concurrent.Semaphore;

/**
 * This class is responsible for managing subscriptions and redirect published Updates on the server..
 * Implements the IBroker interface and is used ad an RemoteObject.
 */
public class Broker extends UnicastRemoteObject implements IBroker {


    private String ftsKnowledge = "";

    // Semaphores to make sure the publish/subscribe methods are only executed completely by one thread at a time
    // Even when called at the same time from different clients.
    static Semaphore semaphorePub = new Semaphore(1);
    static Semaphore semaphoreSub = new Semaphore(1);

    // HashSet used. Prevents duplicate subscriptions, as the stubs referring to remote objects will have the same hash code.
    // See https://docs.oracle.com/en/java/javase/19/docs/api/java.rmi/java/rmi/server/RemoteObject.html#hashCode()
    HashSet<ISubscriber> subscriberSet = new HashSet<>();

    public Broker() throws RemoteException {}

    /**
     * Implementation of the  publish method. Updates the knowledge base on the server and sends the message further
     * to the subscribers
     * @param ftsObject String in XML-Format containing updates about manufacturing technology scouting
     */
    @Override
    public void publish(String ftsObject) {
        try {
            semaphorePub.acquire();
            try{
                System.out.println("Neues Update von Publisher erhalten!");
                this.updateKnowledgeBase(ftsObject);
                this.informSubscribers(ftsObject);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                semaphorePub.release();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Implementation of the subscribe method. It adds the specified subscriber to the subscriberSet.
     * @param subscriber RemoteObject from the subscribing client providing methods to call on updates
     */
    @Override
    public void subscribe(ISubscriber subscriber) {
        try {
            semaphoreSub.acquire();
            try{
                subscriberSet.add(subscriber);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                semaphoreSub.release();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void updateKnowledgeBase(String ftsUpdate) {
        ftsKnowledge += ftsUpdate;
        System.out.println("Servereigene Knowledge Base geupdated!");
    }

    private void informSubscribers(String ftsUpdate) {
        subscriberSet.forEach(subscriber -> {
            try {
                subscriber.notify(ftsUpdate);
            } catch (RemoteException e) {
                System.out.println("Some subscribers could not be reached:");
                System.out.println(e.getMessage());
            }
        });
        System.out.println("Updates wurden an Subscriber weitergeleitet!");
    }
}
