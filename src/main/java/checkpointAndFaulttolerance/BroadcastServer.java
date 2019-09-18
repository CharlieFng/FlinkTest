package checkpointAndFaulttolerance;

import window.tumbling.processTime.SocketProducer;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class BroadcastServer
{
    public static void main(String[] args) throws IOException
    {
        ServerSocket listener = new ServerSocket(9090);
        BufferedReader br = null;
        try
        {
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());

            ClassLoader classLoader = SocketProducer.class.getClassLoader();
            File file = new File(classLoader.getResource("inputs/broadcast_small.txt").getFile());
            InputStream inputStream = new FileInputStream(file);

            br = new BufferedReader(new InputStreamReader(inputStream));

            try
            {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String line;

                while((line = br.readLine()) != null)
                {
                    out.println(line);
                    //Thread.sleep(100);
                }		    }

            finally
            {
                socket.close();
            }

        } catch(Exception e )
        {
            e.printStackTrace();
        } finally
        {
            listener.close();
            if (br != null)
                br.close();
        }    }
}


