import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Properties;

@WebServlet("/produce")
public class Producer extends HttpServlet {
    private static final long serialVersionUID = 1L;
	private KafkaProducer<String, String> producer;

    @Override
    public void init() throws ServletException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String topic = request.getParameter("topic");
        String value = request.getParameter("value");
        String key = request.getParameter("key");
        
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key ,value);
        producer.send(record);
        
        response.getWriter().write("Message sent to Kafka topic " + topic);
    }

    @Override
    public void destroy() {
        producer.close();
    }
}
