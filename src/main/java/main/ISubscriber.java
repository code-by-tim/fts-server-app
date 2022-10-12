package main;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * This interface is needed for the class Subscriber, which is to be used as a remote object.
 * It is declaring methods to be called by the server when contacting the subscribers.
 */
public interface ISubscriber extends Remote {

    /**
     * This method can be called by the server to notify a subscriber of the specified update about
     * manufacturing technology scouting knowledge.
     *
     * @param knowledgeUpdate manufacturing technology scouting knowledge as a String in the standard XML format
     * @throws RemoteException
     */
    void notify(String knowledgeUpdate) throws RemoteException;
}
