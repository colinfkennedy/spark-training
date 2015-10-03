package com.typesafe.training.sws.ex8.wikichanges

import java.io.PrintStream
import java.net.ServerSocket

import scala.collection.JavaConverters._

import io.socket.{SocketIOException, IOAcknowledge, IOCallback, SocketIO}
import org.json.JSONObject

/**
 * Uses socket.io to connect to wikimedia stream and subscribes to live wiki change events.Then the events are modified and published
 * through socket.
 */
object WikiChangesBackend {

  def main(args: Array[String]): Unit ={

    val server = new ServerSocket(8124)
    println("Waiting for a client connection...")
    val clientSocket = server.accept()
    val outStream = new PrintStream(clientSocket.getOutputStream())
    println("Connecting to live wikimedia stream...You can hit any key to stop")
    val socket = new SocketIO("http://stream.wikimedia.org/rc")
    socket.connect(new IOCallback() {
      //Called when the socket disconnects and there are no further attempts to reconnect
      override def onDisconnect(): Unit = {
        println("Connection is disconnected...")
        outStream.close()
        clientSocket.close()
        server.close()
      }

      override def onError(e: SocketIOException): Unit = println(">>>>>>>>> Error " + e)
      override def onMessage(s: String, ioAcknowledge: IOAcknowledge): Unit = ()
      override def onMessage(jsonObject: JSONObject, ioAcknowledge: IOAcknowledge): Unit = ()

      override def onConnect(): Unit = {
        println("Connected to wikimedia stream")
        socket.emit("subscribe", "*.wikipedia.org")
      }
      override def on(s: String, ioAcknowledge: IOAcknowledge, objects: AnyRef*): Unit = {
        //only interested in change events
        if(s != "change") return ()

        objects foreach { message =>
          val json = message.asInstanceOf[JSONObject]
          val pageUrl = json.getString("server_url") + "/wiki/" + json.getString("title").replaceAll(" ", "_")
          val newJson = new JSONObject(Map(
            "user"  -> json.getString("user"),
            "pageUrl" -> pageUrl,
            "id" -> json.getString("id"),
            "timestamp" -> json.getLong("timestamp")).asJava)
          outStream.println(newJson.toString)
          outStream.flush()
        }
      }
    })

    Console.in.readLine()
    socket.disconnect()
  }
}
