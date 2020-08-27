package pb.protocols.keepalive;

import java.time.Instant;
import java.util.logging.Logger;

import pb.Endpoint;
import pb.EndpointUnavailable;
import pb.Manager;
import pb.Utils;
import pb.protocols.ICallback;
import pb.protocols.Message;
import pb.protocols.Protocol;
import pb.protocols.IRequestReplyProtocol;

/**
 * Provides all of the protocol logic for both client and server to undertake
 * the KeepAlive protocol. In the KeepAlive protocol, the client sends a
 * KeepAlive request to the server every 20 seconds using
 * {@link pb.Utils#setTimeout(pb.protocols.ICallback, long)}. The server must
 * send a KeepAlive response to the client upon receiving the request. If the
 * client does not receive the response within 20 seconds (i.e. at the next time
 * it is to send the next KeepAlive request) it will assume the server is dead
 * and signal its manager using
 * {@link pb.Manager#endpointTimedOut(Endpoint,Protocol)}. If the server does
 * not receive a KeepAlive request at least every 20 seconds (again using
 * {@link pb.Utils#setTimeout(pb.protocols.ICallback, long)}), it will assume
 * the client is dead and signal its manager. Upon initialisation, the client
 * should send the KeepAlive request immediately, whereas the server will wait
 * up to 20 seconds before it assumes the client is dead. The protocol stops
 * when a timeout occurs.
 *
 * @see {@link pb.Manager}
 * @see {@link pb.Endpoint}
 * @see {@link pb.protocols.Message}
 * @see {@link pb.protocols.keepalive.KeepAliveRequest}
 * @see {@link pb.protocols.keepalive.KeepAliveReply}
 * @see {@link pb.protocols.Protocol}
 * @see {@link pb.protocols.IRequestReplyProtocol}
 * @author aaron
 *
 */
public class KeepAliveProtocol extends Protocol implements IRequestReplyProtocol {
	private static final Logger log = Logger.getLogger(KeepAliveProtocol.class.getName());

	/**
	 * Name of this protocol.
	 */
	public static final String protocolName="KeepAliveProtocol";

	private static final long TIMER = 20 * 1000;
	private static final long PADDING = 100; // 100 ms for padding time for in flight delays
	private static final long NS2MS = 1000000;

	private long lastRequest = 0;
	private long lastReply = 0;
	private volatile boolean protocolRunning = false;

	private ICallback cb = null;

	private final ICallback serverCallback = () -> {
		if ((System.nanoTime() - lastRequest)/ NS2MS > (TIMER + PADDING)) {
			log.severe("Client timeout occurred");
			manager.endpointTimedOut(endpoint, this);
			protocolRunning = false;
			return;
		}
		scheduleCallback();

	};

	private final ICallback clientCallback = () -> {

		if((System.nanoTime() - lastReply)/ NS2MS > (TIMER + PADDING)) {
			log.severe("Sever timeout occurred");
			manager.endpointTimedOut(endpoint, this);
			protocolRunning = false;
			return;
		}

		try {
			sendRequest(new KeepAliveRequest());
			scheduleCallback();
		} catch (EndpointUnavailable endpointUnavailable) {
			manager.endpointTimedOut(endpoint, this);
			protocolRunning = false;
		}

	};

	/**
	 * Initialise the protocol with an endopint and a manager.
	 * @param endpoint
	 * @param manager
	 */
	public KeepAliveProtocol(Endpoint endpoint, Manager manager) {
		super(endpoint,manager);
	}

	private void scheduleCallback() {
		Utils.getInstance().setTimeout(cb, TIMER);
	}
	/**
	 * @return the name of the protocol
	 */
	@Override
	public String getProtocolName() {
		return protocolName;
	}

	/**
	 *
	 */
	@Override
	public void stopProtocol() {
		if(protocolRunning) {
			log.severe("Protocol is already running");
		}
	}

	/*
	 * Interface methods
	 */

	/**
	 *
	 */
	public void startAsServer() {
		protocolRunning = true;
		cb = serverCallback;
		scheduleCallback();
		lastRequest  = System.nanoTime();
	}

	/**
	 *
	 */
	public void startAsClient() throws EndpointUnavailable {
		protocolRunning = true;
		cb = clientCallback;
		sendRequest(new KeepAliveRequest());
		scheduleCallback();
		lastReply = System.nanoTime();
	}

	/**
	 *
	 * @param msg
	 */
	@Override
	public void sendRequest(Message msg) throws EndpointUnavailable {
		endpoint.send(msg);
	}

	/**
	 *
	 * @param msg
	 */
	@Override
	public void receiveReply(Message msg) {
		lastReply = System.nanoTime();
	}

	/**
	 *
	 * @param msg
	 * @throws EndpointUnavailable
	 */
	@Override
	public void receiveRequest(Message msg) throws EndpointUnavailable {
		lastRequest = System.nanoTime();
		sendReply(new KeepAliveReply());
	}

	/**
	 *
	 * @param msg
	 */
	@Override
	public void sendReply(Message msg) throws EndpointUnavailable {
		endpoint.send(msg);
	}


}
