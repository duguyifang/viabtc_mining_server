#include "kafka_producer.h"

static rd_kafka_conf_t *conf_ = NULL;
static rd_kafka_t *producer_ = NULL;
static rd_kafka_topic_t *topic_ = NULL;


static void
kafkaLogger(const rd_kafka_t *rk, int level, const char *fac, const char *buf) {
  printf ("RDKAFKA-%d-%s-%s-%s", level, fac , (producer_ ? rd_kafka_name(rk) : " ") , buf);
}

int kafka_init(char* brokers, char* topic) {
  conf_ = rd_kafka_conf_new();
  rd_kafka_resp_err_t err; /* librdkafka API error code */
  char errstr[512];        /* librdkafka API error reporting buffer */
  char* option [][2] = {
      {"message.max.bytes", RDKAFKA_MESSAGE_MAX_BYTES},
      {"compression.codec", RDKAFKA_COMPRESSION_CODEC},
      {"queue.buffering.max.messages", RDKAFKA_QUEUE_BUFFERING_MAX_MESSAGES},
      {"queue.buffering.max.ms", RDKAFKA_QUEUE_BUFFERING_MAX_MS},
      {"batch.num.messages", RDKAFKA_BATCH_NUM_MESSAGES}
  };

  printf("brokers : %s, topic : %s\n", brokers, topic);

  for(size_t i = 0; i < sizeof(option)/ sizeof(option[0]); i ++) {
    printf("key : %s, value : %s\n", option[i][0], option[i][1]);
    if (rd_kafka_conf_set(conf_, option[i][0], option[i][1],
                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", errstr);
        rd_kafka_conf_destroy(conf_);
        return 1;
    }
  }

  if (rd_kafka_conf_set(conf_, "bootstrap.servers", brokers,
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf_);
                return 1;
  }

  // rd_kafka_conf_set_log_cb(conf_, kafkaLogger);

  producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf_, errstr, sizeof(errstr));
  if (!producer_) {
          fprintf(stderr,
                  "Failed to create new producer: %s\n", errstr);
          return 1;
  }

  rd_kafka_topic_conf_t *topicConf = rd_kafka_topic_conf_new();

  topic_ = rd_kafka_topic_new(producer_, topic, topicConf);

//   checkalive
  const struct rd_kafka_metadata *metadata;
  /* Fetch metadata */
  err = rd_kafka_metadata(
      producer_, topic_ ? 0 : 1, topic_, &metadata, 3000 /* timeout_ms */);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
     printf("Failed to acquire metadata: %s", rd_kafka_err2str(err));
    return -__LINE__;
  } else {
      printf("acquire metadata success!");
  }
    rd_kafka_metadata_destroy(metadata);
  return 0;
}

int send_msg_to_kafka(char* payload, size_t len) {
  if(producer_ == NULL) return -__LINE__;
      // rd_kafka_produce() is non-blocking
  // Returns 0 on success or -1 on error
  int res = rd_kafka_produce(
      topic_,
      RD_KAFKA_PARTITION_UA,
      RD_KAFKA_MSG_F_COPY,
      (void *)payload,
      len,
      NULL,
      0, /* Optional key and its length */
      /* Message opaque, provided in delivery report
       * callback as msg_opaque. */
      NULL);
  if (res == -1) {
    printf("produce to topic [ %s , %s]:", rd_kafka_topic_name(topic_), rd_kafka_err2str(rd_kafka_last_error()));
    return -__LINE__;
  }
  return 0;
}

void kafka_destroy() {
    if (producer_ != NULL) {
    /* Poll to handle delivery reports */
    rd_kafka_poll(producer_, 0);

    /* Wait for messages to be delivered */
    while (rd_kafka_outq_len(producer_) > 0) {
      rd_kafka_poll(producer_, 100);
    }

    if (topic_ != NULL) {
      rd_kafka_topic_destroy(topic_); // Destroy topic
    }
    rd_kafka_destroy(producer_); // Destroy the handle
  }
}


// int main(int argc, char**argv) {
//   kafka_init("127.0.0.1:9092", "RawGbt");
//   return 0;
// }