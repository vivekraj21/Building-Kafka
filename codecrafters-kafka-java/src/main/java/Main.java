import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        System.err.println("Kafka broker server is running...");

        try (ServerSocket serverSocket = new ServerSocket(9092)) { // Start server on port 9092
            serverSocket.setReuseAddress(true); // Allows reusing the port to avoid "Address already in use" errors
            System.out.println("Waiting for client connection...");

            while (true) { // Keep the server running to handle multiple requests
                try (Socket clientSocket = serverSocket.accept()) { // Wait for a client connection
                    System.out.println("Client connected!");

                    // 🔹 Step 1: Set up Input & Output Streams
                    InputStream in = clientSocket.getInputStream();  // Reads data from client
                    OutputStream out = clientSocket.getOutputStream(); // Sends response to client

                    // 🔹 Step 2: Read the request header (fixed-size fields)
                    in.readNBytes(4); // Message Length (4 bytes) - We don't need to store it.
                    in.readNBytes(2); // API Key (2 bytes) - Should be 18 for ApiVersions.

                    // Read API Version (2 bytes)
                    byte[] apiVersionBytes = in.readNBytes(2);
                    
                    // Read Correlation ID (4 bytes) - Unique identifier for request/response tracking
                    byte[] correlationId = in.readNBytes(4);
                    
                    // Read Tagged Fields (1 byte) - Unused metadata (always 0 for now)
                    in.readNBytes(1);

                    // 🔹 Step 3: Convert API Version from bytes to integer
                    short apiVersion = ByteBuffer.wrap(apiVersionBytes).getShort();
                    System.out.println("Requested API Version: " + apiVersion);
                    System.out.println("Correlation ID: " + Arrays.toString(correlationId));

                    // 🔹 Step 4: Build response using ByteArrayOutputStream
                    var bos = new ByteArrayOutputStream(); // Temporary buffer to store response

                    // 🔹 Step 5: Add Correlation ID (Must match the request)
                    bos.write(correlationId);

                    // 🔹 Step 6: Build API Versions Response
                    if (apiVersion < 0 || apiVersion > 4) {
                        // If API version is invalid, return error code 35 (unsupported version)
                        bos.write(new byte[]{0, 35}); // Error Code (2 bytes)
                    } else {
                        // ✅ Valid API Version → Build successful response
                        bos.write(new byte[]{0, 0});   // Error Code (0 = No Error)
                        bos.write(2);                  // Number of API entries (1 + reserved space)
                        bos.write(new byte[]{0, 18});  // API Key (18 = ApiVersions)
                        bos.write(new byte[]{0, 3});   // Min Version (v3)
                        bos.write(new byte[]{0, 4});   // Max Version (v4)
                        bos.write(0);                  // Tagged Fields (unused, always 0)
                        bos.write(new byte[]{0, 0, 0, 0}); // Throttle Time (default 0 = no delay)
                        bos.write(0);                  // Tagged Fields again (unused, always 0)
                    }

                    // 🔹 Step 7: Compute message length & prepend it
                    byte[] response = bos.toByteArray(); // Convert response to byte array
                    byte[] sizeBytes = ByteBuffer.allocate(4).putInt(response.length).array(); // Compute 4-byte length

                    // Debugging: Print response details
                    System.out.println("Size Bytes: " + Arrays.toString(sizeBytes));
                    System.out.println("Response: " + Arrays.toString(response));

                    // 🔹 Step 8: Send response to client
                    out.write(sizeBytes); // First send message size (4 bytes)
                    out.write(response);  // Then send actual response
                    out.flush();          // Ensure all data is sent

                    System.out.println("Response sent successfully!");
                } catch (IOException e) {
                    System.err.println("IOException: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }
}
