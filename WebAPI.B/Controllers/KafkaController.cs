using System.Net;
using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace WebAPI.B.Controllers
{
    [Route("api/[controller]/b")]
    [ApiController]
    public class KafkaController : ControllerBase
    {
        private readonly ProducerConfig _producerConfig;
        public KafkaController()
        {
            _producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = Dns.GetHostName()
            };
        }

        [HttpPost]
        public async Task<IActionResult> Produce([FromBody] string message)
        {
            using (var producer = new ProducerBuilder<Null, string>(_producerConfig).Build())
            {
               await producer.ProduceAsync(ObjectOfValue.Constants.TopicB, new Message<Null, string> { Value = message });
            }

            return Ok();
        }
    }
}
