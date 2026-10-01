#include "mros2.h"

#include <rtps/rtps.h>
#include <iostream> // debug用　削除予定

#ifdef __MBED__
#include "mbed.h"
#else /* __MBED__ */
#include "mros2_config.h"
#include "cmsis_os.h"
#endif /* __MBED__ */

// for service
#include <condition_variable>
#include <map>
#include <future>
#include <memory>
#include <queue>
#include <thread>
#include <vector>
// #include "rtps/common/types.h" //sample_identify struct

// std::mutex mtx;             // wait で必要（とりあえず無視）
// std::condition_variable cv; // これを介してスリープをコントロールする
std::map<uint32_t, std::promise<std::vector<uint8_t>>> promise_map;
// newChangeIdentify() が送信を開始してからpromise_mapへ登録するまでの
// 短い区間に、ローカル/高速なサービス応答が届くことがある。その応答を
// 捨てずに一時保持し、要求側の登録完了後に渡す。
std::map<uint32_t, std::vector<uint8_t>> early_service_responses;
constexpr size_t kMaxEarlyServiceResponses = 16;
std::mutex map_mutex;
// for service server
std::queue<rtps::Sample_Indetify> request_identify_queue;
std::mutex queue_mutex;

namespace mros2
{

  rtps::Domain *domain_ptr = NULL;
  rtps::Participant *part_ptr = NULL; // TODO: detele this
  rtps::Writer *pub_ptr = NULL;

#define SUB_MSG_SIZE 4 // addr size
  osMessageQueueId_t subscriber_msg_queue_id;

  bool completeNodeInit = false;
  uint8_t endpointId = 0;
  uint32_t subCbArray[10];

  uint8_t buf[100], frag_buf[64];
  // uint8_t buf[1000], frag_buf[1000];
  uint8_t buf_index = 4;

  /* Callback function to set the boolean to true upon a match */
  void setTrue(void *args)
  {
    *static_cast<volatile bool *>(args) = true;
  }

  bool subMatched = false;
  bool pubMatched = false;

  bool client_subMatched = false;
  bool client_pubMatched = false;

  // for service
  rtps::Sample_Indetify request_identify;

  void pubMatch(void *args)
  {
    MROS2_DEBUG("[MROS2LIB] publisher matched with remote subscriber");
  }

  void subMatch(void *args)
  {
    MROS2_DEBUG("[MROS2LIB] subscriber matched with remote publisher");
  }

  void client_pubMatch(void *args)
  {
    client_pubMatched = true;
    MROS2_DEBUG("[MROS2LIB] [service] client:publisher matched with remote subscriber");
  }

  void client_subMatch(void *args)
  {
    client_subMatched = true;
    MROS2_DEBUG("[MROS2LIB] [service] client:subscriber matched with remote publisher");
  }

/*
 *  Initialization of mROS 2 environment
 */
#ifdef __MBED__
  Thread *mros2_init_thread;
#endif /* __MBED__ */
  // init threadを開始する
  void init(int argc, char *argv[])
  {
    buf[0] = 0;
    buf[1] = 1;
    buf[2] = 0;
    buf[3] = 0;

#ifdef __MBED__
    mros2_init_thread = new Thread(osPriorityAboveNormal, 5000, nullptr, "mROS2Thread");
    mros2_init_thread->start(callback(mros2_init, (void *)NULL));
#else  /* __MBED__ */
    osThreadAttr_t attributes;

    attributes.name = "mROS2Thread",
    attributes.stack_size = 5000,
    attributes.priority = (osPriority_t)24,

    osThreadNew(mros2_init, NULL, (const osThreadAttr_t *)&attributes);
#endif /* __MBED__ */
  }

  void mros2_init(void *args)
  {
    osStatus_t ret;

    MROS2_DEBUG("[MROS2LIB] mros2_init task start");

#ifndef __MBED__
    MX_LWIP_Init();
    MROS2_DEBUG("[MROS2LIB] Initilizing lwIP complete");
#endif /* __MBED__ */

    static rtps::Domain domain;
    domain_ptr = &domain;

#ifndef __MBED__
    subscriber_msg_queue_id = osMessageQueueNew(SUB_MSG_COUNT, SUB_MSG_SIZE, NULL);
    if (subscriber_msg_queue_id == NULL)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: mROS2 init failed");
      return;
    }
#endif /* __MBED__ */

    /* wait until participant(node) is created */
    while (!completeNodeInit)
    {
      osDelay(100);
    }
    domain.completeInit();
    MROS2_DEBUG("[MROS2LIB] Initilizing Domain complete");

    while (!subMatched && !pubMatched)
    {
      osDelay(1000);
    }

    MROS2_DEBUG("[MROS2LIB] mros2_init task end");

    ret = osThreadTerminate(NULL);
    if (ret != osOK)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: mros2_init() task terminate error %d", ret);
    }
  }

  /*
   *  Node functions
   */
  Node Node::create_node(std::string node_name)
  {
    MROS2_DEBUG("[MROS2LIB] create_node");
    MROS2_DEBUG("[MROS2LIB] start creating participant");

    while (domain_ptr == NULL)
    {
      osDelay(100);
    }

    Node node;
    node.part = domain_ptr->createParticipant();
    /* TODO: utilize node name */
    node.node_name = node_name;
    part_ptr = node.part;
    if (node.part == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: create_node() failed");
      while (true)
      {
      }
    }
    completeNodeInit = true;

    MROS2_DEBUG("[MROS2LIB] successfully created participant");
    return node;
  }

  // ReaderCacheChange owns its data only for the duration of the reader
  // callback.  A callback worker must therefore receive an owned copy, not a
  // pointer into ReaderCacheChange.
  class SubscriberCallbackWorker
  {
  public:
    virtual ~SubscriberCallbackWorker() = default;
    virtual void enqueue(const uint8_t *data, size_t size) = 0;
  };

  // One worker is created per subscription.  Its single message slot is
  // deliberately overwrite-on-write: for state-like topics, running the
  // newest callback is preferable to accumulating stale callbacks and it
  // bounds both memory use and callback latency.
  template <class T>
  class LatestMessageCallbackWorker final : public SubscriberCallbackWorker
  {
  public:
    explicit LatestMessageCallbackWorker(void (*callback)(intptr_t))
        : callback_(callback), worker_(&LatestMessageCallbackWorker::run, this)
    {
    }

    ~LatestMessageCallbackWorker() override
    {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
      }
      condition_.notify_one();
      if (worker_.joinable())
      {
        worker_.join();
      }
    }

    void enqueue(const uint8_t *data, size_t size) override
    {
      if (data == nullptr || size < SUB_MSG_SIZE)
      {
        MROS2_ERROR("[MROS2LIB] received malformed subscription payload");
        return;
      }

      {
        std::lock_guard<std::mutex> lock(mutex_);
        // Capacity one / latest-value-wins. assign() copies the RTPS frame
        // before ReaderCacheChange and its backing storage leave scope.
        latest_message_.assign(data, data + size);
        has_message_ = true;
      }
      condition_.notify_one();
    }

  private:
    void run()
    {
      for (;;)
      {
        std::vector<uint8_t> message;
        {
          std::unique_lock<std::mutex> lock(mutex_);
          condition_.wait(lock, [this] { return stopping_ || has_message_; });
          if (stopping_ && !has_message_)
          {
            return;
          }
          message.swap(latest_message_);
          has_message_ = false;
        }

        T msg;
        const uint8_t *data = message.data();
        msg.copyFromBuf(&data[SUB_MSG_SIZE]);
        callback_((intptr_t)&msg);
      }
    }

    void (*callback_)(intptr_t);
    std::mutex mutex_;
    std::condition_variable condition_;
    std::vector<uint8_t> latest_message_;
    bool has_message_ = false;
    bool stopping_ = false;
    std::thread worker_;
  };

  typedef struct
  {
    void (*cb_fp)(intptr_t);
    void (*cb_fp_srv)(intptr_t, intptr_t);
    intptr_t argp;
    rtps::Writer *pub_ptr_own;
    // create_client() の Reply reader はpromiseを解決するだけで、T は
    // Request 型である。Response frameをTとしてデシリアライズしてはいけない。
    bool service_response_only = false;
    std::unique_ptr<SubscriberCallbackWorker> callback_worker;
  } SubscribeDataType;

  // typedef struct
  // {
  //   void (*cb_fp_srv)(intptr_t, intptr_t);
  //   intptr_t argp;
  // } SubscribeServiceDataType;

  /*
   *  service releated create functions
   */
  void response_received_processingrCallback(uint8_t *msg)
  {
    // printf("subscribed msg: '%s'\r\n", msg->data.c_str());
    // printf("subscribed msg: calculation sum:'%ld'\r\n", msg->data);
  }

  int wait_service(int timeout)
  {
    int time_sec = timeout * 1000;
    while (!client_subMatched && !client_pubMatched)
    {
      MROS2_DEBUG("[MROS2LIB] waiting for service to be matched");
      osDelay(time_sec);
    }
    return 0;
  }

  template <class T>
  Publisher Node::create_client(std::string topic_name, int qos)
  {

    rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rq/" + topic_name + "Request").c_str(), message_traits::TypeName<T *>().request_type(), true);

    if (writer == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create writer in create_publisher()");
      while (true)
      {
      }
    }

    Publisher pub; // publish宣言も含む
    pub_ptr = writer;
    pub.pub_ptr_own = writer;
    pub.topic_name = topic_name; // この設定は何に使う？間違った値が入っていても動く

    /* Register callback to ensure that a publisher is matched to the writer before sending messages */
    part_ptr->registerOnNewSubscriberMatchedCallback(client_pubMatch, &subMatched);
    MROS2_DEBUG("[MROS2LIB] create_publisher complete.");

    /*
    init service subsctiber
    */
    // mros2::Subscriber sub = create_client_subscription<T>("add_two_ints", 10, response_received_processingrCallback, message_traits::TypeName<T *>().response_type());

    rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rr/" + topic_name + "Reply").c_str(), message_traits::TypeName<T *>().response_type(), true); // for add_two_ints
    if (reader == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create reader in create_subscription()");
      while (true)
      {
      }
    }

    Subscriber sub;
    sub.topic_name = topic_name; // この設定は何に使う？
    // void *fp = response_received_processingrCallback; // 一旦定義
    void (*fp)(uint8_t *) = response_received_processingrCallback;
    sub.cb_fp = (void (*)(intptr_t))fp; // 関数の扱い

    SubscribeDataType *data_p;
    data_p = new SubscribeDataType;
    data_p->cb_fp = (void (*)(intptr_t))fp;
    data_p->argp = (intptr_t)NULL;
    // Tはservice request型、Readerが受けるのはresponse型であるため、
    // ここではTへのデシリアライズを行わない。callback_handlerがfutureを
    // 完了した時点で返す。
    data_p->service_response_only = true;
    reader->registerCallback(sub.callback_handler<T>, (void *)data_p); // templeteの関数を登録

    /* Register callback to ensure that a subscriber is matched to the reader before receiving messages */
    part_ptr->registerOnNewPublisherMatchedCallback(client_subMatch, &pubMatched);

    MROS2_DEBUG("[MROS2LIB] create_subscription complete.");
    // return sub;

    return pub;
  }

  int spin_until_future_complete(Node node,
                                 std::future<std::vector<uint8_t>> *response)
  {
    response->wait();
    return 0;
  }

  void cancel_service_request(uint32_t request_id)
  {
    std::promise<std::vector<uint8_t>> promise;
    bool found = false;
    {
      std::lock_guard<std::mutex> lock(map_mutex);
      auto it = promise_map.find(request_id);
      if (it != promise_map.end())
      {
        promise = std::move(it->second);
        promise_map.erase(it);
        found = true;
      }
    }

    if (found)
    {
      // promiseを未完了のまま破棄すると、std::promiseは待機futureへ
      // broken_promise例外を設定する。-fno-exceptionsのWASMではそれが
      // unreachableになるため、timeoutは空フレームで正常完了させる。
      promise.set_value({});
    }
  }

  // 現状client側のpublisherのみ対応
  template <class T>
  Publisher Node::create_client_publisher(std::string topic_name, int qos, std::string type_name)
  {
    // rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rq/" + topic_name).c_str(), message_traits::TypeName<T *>().value(), false);
    // add_two_intsようにtype_nameを指定
    // rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rq/" + topic_name).c_str(), "example_interfaces::srv::dds_::AddTwoInts_Request_", false);
    // reliablity=True
    // rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rq/" + topic_name + "Request").c_str(), "example_interfaces::srv::dds_::AddTwoInts_Request_", true);
    rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rq/" + topic_name + "Request").c_str(), type_name.c_str(), true);
    // rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rq/" + topic_name).c_str(), "CalculatorRequestType", false); // fastdds-exempleようにtype_nameを指定

    if (writer == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create writer in create_publisher()");
      while (true)
      {
      }
    }

    Publisher pub; // publish宣言も含む
    pub_ptr = writer;
    pub.topic_name = topic_name;

    /* Register callback to ensure that a publisher is matched to the writer before sending messages */
    part_ptr->registerOnNewSubscriberMatchedCallback(pubMatch, &subMatched);

    MROS2_DEBUG("[MROS2LIB] create_publisher complete.");
    return pub;
  }

  // 現状client側のsubscriberのみ対応
  template <class T>
  Subscriber Node::create_client_subscription(std::string topic_name, int qos, void (*fp)(T *), std::string type_name)
  {
    // rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rr/" + topic_name).c_str(), message_traits::TypeName<T *>().value(), false);
    // rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rr/" + topic_name + "Reply").c_str(), "example_interfaces::srv::dds_::AddTwoInts_Response_", true); // for add_two_ints
    rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rr/" + topic_name + "Reply").c_str(), type_name.c_str(), true); // for add_two_ints
    if (reader == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create reader in create_subscription()");
      while (true)
      {
      }
    }

    Subscriber sub;
    sub.topic_name = topic_name;
    sub.cb_fp = (void (*)(intptr_t))fp;

    SubscribeDataType *data_p;
    data_p = new SubscribeDataType;
    data_p->cb_fp = (void (*)(intptr_t))fp;
    data_p->argp = (intptr_t)NULL;
    data_p->callback_worker.reset(
        new LatestMessageCallbackWorker<T>(data_p->cb_fp));
    reader->registerCallback(sub.callback_handler<T>, (void *)data_p);

    /* Register callback to ensure that a subscriber is matched to the reader before receiving messages */
    part_ptr->registerOnNewPublisherMatchedCallback(subMatch, &pubMatched);

    MROS2_DEBUG("[MROS2LIB] create_subscription complete.");
    return sub;
  }

  template <class T, class U>
  Subscriber Node::create_service(std::string topic_name, int qos, void (*fp)(T *, U *))
  {
    rtps::Writer *writer = domain_ptr->createWriter(*(this->part), ("rr/" + topic_name + "Reply").c_str(), message_traits::TypeName<T *>().response_type(), true);
    if (writer == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create writer in create_publisher()");
      while (true)
      {
      }
    }

    Publisher pub; // publish宣言も含む
    pub_ptr = writer;

    pub.topic_name = topic_name; // この設定は何に使う？間違った値が入っていても動く

    /* Register callback to ensure that a publisher is matched to the writer before sending messages */
    part_ptr->registerOnNewSubscriberMatchedCallback(pubMatch, &subMatched);
    MROS2_DEBUG("[MROS2LIB] create_publisher complete.");

    rtps::Reader *reader = domain_ptr->createReader(*part_ptr, ("rq/" + topic_name + "Request").c_str(), message_traits::TypeName<T *>().request_type(), true); // for add_two_ints
    if (reader == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create reader in create_subscription()");
      while (true)
      {
      }
    }
    Subscriber sub;
    sub.topic_name = topic_name;
    // sub.pub_ptr_own = writer;
    // sub.cb_fp = (void (*)(intptr_t))fp; // なぜコールバック登録が２つあるのか？
    sub.cb_fp_srv = (void (*)(intptr_t, intptr_t))fp;

    SubscribeDataType *data_p;
    data_p = new SubscribeDataType;
    // data_p->cb_fp = (void (*)(intptr_t))fp;
    data_p->cb_fp_srv = (void (*)(intptr_t, intptr_t))fp;
    data_p->argp = (intptr_t)NULL;
    data_p->pub_ptr_own = writer;
    reader->registerCallback(sub.service_callback_handler<T, U>, (void *)data_p);

    /* Register callback to ensure that a subscriber is matched to the reader before receiving messages */
    part_ptr->registerOnNewPublisherMatchedCallback(subMatch, &pubMatched);

    MROS2_DEBUG("[MROS2LIB] create_subscription complete.");
    return sub;
  }

  /*service server function*/
  template <class T>
  Publisher Node::create_service_server_publisher(std::string topic_name, int qos)
  {

    rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rr/" + topic_name).c_str(), "example_interfaces::srv::dds_::AddTwoInts_Response_", true);

    if (writer == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create writer in create_publisher()");
      while (true)
      {
      }
    }

    Publisher pub;
    pub_ptr = writer;
    pub.topic_name = topic_name;

    /* Register callback to ensure that a publisher is matched to the writer before sending messages */
    part_ptr->registerOnNewSubscriberMatchedCallback(pubMatch, &subMatched);

    MROS2_DEBUG("[MROS2LIB] create_publisher complete.");
    return pub;
  }

  template <class T, class U>
  Subscriber Node::create_service_server_subscription(std::string topic_name, int qos, void (*fp)(T *, U *))
  {
    // rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rr/" + topic_name).c_str(), message_traits::TypeName<T *>().value(), false);
    rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rq/" + topic_name).c_str(), "example_interfaces::srv::dds_::AddTwoInts_Request_", true); // for add_two_ints

    if (reader == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create reader in create_subscription()");
      while (true)
      {
      }
    }

    Subscriber sub;
    sub.topic_name = topic_name;
    // sub.cb_fp = (void (*)(intptr_t))fp; // なぜコールバック登録が２つあるのか？
    sub.cb_fp_srv = (void (*)(intptr_t, intptr_t))fp;

    SubscribeDataType *data_p;
    data_p = new SubscribeDataType;
    // data_p->cb_fp = (void (*)(intptr_t))fp;
    data_p->cb_fp_srv = (void (*)(intptr_t, intptr_t))fp;
    data_p->argp = (intptr_t)NULL;
    reader->registerCallback(sub.service_callback_handler<T, U>, (void *)data_p);

    /* Register callback to ensure that a subscriber is matched to the reader before receiving messages */
    part_ptr->registerOnNewPublisherMatchedCallback(subMatch, &pubMatched);

    MROS2_DEBUG("[MROS2LIB] create_subscription complete.");
    return sub;
  }

  /*service debug */
  // template <class T>
  // Subscriber Node::create_service_subscription_debug(std::string topic_name, int qos, void (*fp)(T *))
  // {
  //   // rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rr/" + topic_name).c_str(), message_traits::TypeName<T *>().value(), false);
  //   rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rt/" + topic_name).c_str(), "example_interfaces::srv::dds_::AddTwoInts_Response_", true); // for add_two_ints
  //   if (reader == nullptr)
  //   {
  //     MROS2_ERROR("[MROS2LIB] ERROR: failed to create reader in create_subscription()");
  //     while (true)
  //     {
  //     }
  //   }

  //   Subscriber sub;
  //   sub.topic_name = topic_name;
  //   sub.cb_fp = (void (*)(intptr_t))fp;

  //   SubscribeDataType *data_p;
  //   data_p = new SubscribeDataType;
  //   data_p->cb_fp = (void (*)(intptr_t))fp;
  //   data_p->argp = (intptr_t)NULL;
  //   reader->registerCallback(sub.callback_handler<T>, (void *)data_p);

  //   /* Register callback to ensure that a subscriber is matched to the reader before receiving messages */
  //   part_ptr->registerOnNewPublisherMatchedCallback(subMatch, &pubMatched);

  //   MROS2_DEBUG("[MROS2LIB] create_subscription complete.");
  //   return sub;
  // }

  /*
   *  Publisher functions
   */
  template <class T>
  Publisher Node::create_publisher(std::string topic_name, int qos)
  {
    rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rt/" + topic_name).c_str(), message_traits::TypeName<T *>().value(), false);
    // debug
    std::cout << "[DEBUG] message_traits::TypeName<T *>().value(): " << message_traits::TypeName<T *>().value() << std::endl;
    MROS2_DEBUG("[MROS2LIB] message_traits::TypeName<T *>().value() %s", message_traits::TypeName<T *>().value());

    if (writer == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create writer in create_publisher()");
      while (true)
      {
      }
    }

    Publisher pub;
    pub_ptr = writer;
    pub.pub_ptr_own = writer;
    pub.topic_name = topic_name;

    /* Register callback to ensure that a publisher is matched to the writer before sending messages */
    part_ptr->registerOnNewSubscriberMatchedCallback(pubMatch, &subMatched);

    MROS2_DEBUG("[MROS2LIB] create_publisher complete.");
    return pub;
  }

  template <class T>
  std::future<std::vector<uint8_t>> Publisher::async_send_request(
      T &msg, uint32_t *request_id)
  {
    auto func = [&msg]
    {
      rtps::DataSize_t len = 0;
      rtps::DataSize_t mod_len = 0;
      size_t cdr_enc_offset = 0;

      if (0 == msg.getPubCnt())
      {
        cdr_enc_offset = 4;
        frag_buf[0] = 0;
        frag_buf[1] = 1;
        frag_buf[2] = 0;
        frag_buf[3] = 0;
      }

      auto ret = msg.copyToFragBuf(&frag_buf[cdr_enc_offset],
                                   sizeof(frag_buf) - cdr_enc_offset);
      len = ret.second + cdr_enc_offset;
      if (ret.first)
      {
        if (0 < ret.second)
        {
          mod_len = len % 4;
          if (mod_len > 0)
          {
            for (int i = 0; i < (4 - mod_len); i++)
            {
              frag_buf[len++] = 0;
            }
          }
        }
        else
        {
          msg.resetCount();
        }
      }

      return std::make_pair(frag_buf, (rtps::DataSize_t)(len));
    };

    // messege size が指定地より大きい場合は、nullpointerを返す関数を呼ぶ=送信しない
    if (sizeof(buf) < msg.calcTotalSize())
    {
      pub_ptr->newChangeCallback(rtps::ChangeKind_t::ALIVE,
                                 func, msg.calcTotalSize());
    }
    else
    {
      // Service requests can be emitted while another publisher is using the
      // process-wide serialization buffer.  Keep the complete CDR payload on
      // this call's stack so a concurrent topic publish cannot corrupt the
      // encapsulation header or request fields before addChange() copies it.
      uint8_t request_buf[sizeof(buf)] = {0, 1, 0, 0};
      msg.copyToBuf(&request_buf[4]);
      msg.memAlign(&request_buf[4]);
      // Generated message getTotalSize() is destructive: it returns cntPub
      // and resets that counter to zero.  Calling it once for logging and
      // again for addChange() used to store only the four-byte CDR header,
      // dropping the complete service request body from the RTPS DATA sample.
      const rtps::DataSize_t serialized_payload_size =
          static_cast<rtps::DataSize_t>(msg.getTotalSize());
      MROS2_DEBUG("[MROS2LIB] service request CDR header: %02x %02x %02x %02x payload_bytes=%u",
                  request_buf[0], request_buf[1], request_buf[2], request_buf[3],
                  static_cast<unsigned int>(serialized_payload_size));
      // pub_ptr->newChange(rtps::ChangeKind_t::ALIVE, buf,
      //                    msg.getTotalSize() + 4);
      // rtps::CacheChange *result = const_cast<rtps::CacheChange *>(pub_ptr->newChange(rtps::ChangeKind_t::ALIVE, buf, msg.getTotalSize() + 4));
      // newChangeIdentify() fills this empty identity with the request
      // writer's GUID and the newly allocated RTPS sequence number.  Reusing
      // a fixed SampleIdentity makes all ROS 2 service requests look like the
      // same request to the server.
      rtps::Sample_Indetify identify{};
      // rtps::CacheChange *result = const_cast<rtps::CacheChange *>(pub_ptr->newChangeIdentify(rtps::ChangeKind_t::ALIVE, buf, msg.getTotalSize() + 4, identify));
      rtps::CacheChange *result = const_cast<rtps::CacheChange *>(
          pub_ptr_own->newChangeIdentify(rtps::ChangeKind_t::ALIVE,
                                        request_buf,
                                        serialized_payload_size + 4,
                                        identify));
      // for service通信
      // sequenceNumberを取得して、別の変数にセット
      SequenceNumber_t sequenceNumber_pub = result->sequenceNumber;
      MROS2_DEBUG("[MROS2LIB] sequenceNumber_pub: %d", sequenceNumber_pub.low);
      if (request_id != nullptr)
      {
        *request_id = sequenceNumber_pub.low;
      }

      // promise
      std::promise<std::vector<uint8_t>> promise;
      auto future = promise.get_future();
      std::vector<uint8_t> early_response;
      bool has_early_response = false;
      {
        std::lock_guard<std::mutex> lock(map_mutex);
        auto early = early_service_responses.find(sequenceNumber_pub.low);
        if (early != early_service_responses.end())
        {
          early_response = std::move(early->second);
          early_service_responses.erase(early);
          has_early_response = true;
        }
        else
        {
          promise_map[sequenceNumber_pub.low] = std::move(promise);
        }
      }
      if (has_early_response)
      {
        MROS2_DEBUG("[MROS2LIB] using early service response: id=%u",
                    sequenceNumber_pub.low);
        promise.set_value(std::move(early_response));
      }
      return future;
    }
  }

  template <class T>
  void Publisher::publish(T &msg)
  {
    auto func = [&msg]
    {
      rtps::DataSize_t len = 0;
      rtps::DataSize_t mod_len = 0;
      size_t cdr_enc_offset = 0;

      if (0 == msg.getPubCnt())
      {
        cdr_enc_offset = 4;
        frag_buf[0] = 0;
        frag_buf[1] = 1;
        frag_buf[2] = 0;
        frag_buf[3] = 0;
      }

      auto ret = msg.copyToFragBuf(&frag_buf[cdr_enc_offset],
                                   sizeof(frag_buf) - cdr_enc_offset);
      len = ret.second + cdr_enc_offset;
      if (ret.first)
      {
        if (0 < ret.second)
        {
          mod_len = len % 4;
          if (mod_len > 0)
          {
            for (int i = 0; i < (4 - mod_len); i++)
            {
              frag_buf[len++] = 0;
            }
          }
        }
        else
        {
          msg.resetCount();
        }
      }

      return std::make_pair(frag_buf, (rtps::DataSize_t)(len));
    };

    // messege size が指定地より大きい場合は、nullpointerを返す関数を呼ぶ=送信しない
    if (sizeof(buf) < msg.calcTotalSize())
    {
      pub_ptr->newChangeCallback(rtps::ChangeKind_t::ALIVE,
                                 func, msg.calcTotalSize());
    }
    else
    {
      msg.copyToBuf(&buf[4]);
      msg.memAlign(&buf[4]);
      if (!request_identify_queue.empty())
      {
        rtps::Sample_Indetify identify = request_identify_queue.front();
        // front_sampleで処理を行う
        request_identify_queue.pop(); // 先頭要素を削除
        rtps::CacheChange *result = const_cast<rtps::CacheChange *>(pub_ptr->newChangeIdentify(rtps::ChangeKind_t::ALIVE, buf, msg.getTotalSize() + 4, identify));
        SequenceNumber_t sequenceNumber_pub = result->sequenceNumber;
        MROS2_DEBUG("[MROS2LIB] sequenceNumber_pub: %d", sequenceNumber_pub.low);
        return;
      }
      // pub_ptr->newChange(rtps::ChangeKind_t::ALIVE, buf,
      //                    msg.getTotalSize() + 4);
      pub_ptr_own->newChange(rtps::ChangeKind_t::ALIVE, buf,
                             msg.getTotalSize() + 4);
      return;
      // rtps::CacheChange *result = const_cast<rtps::CacheChange *>(pub_ptr->newChange(rtps::ChangeKind_t::ALIVE, buf, msg.getTotalSize() + 4));

      // rtps::CacheChange *result = const_cast<rtps::CacheChange *>(pub_ptr->newChangeIdentify(rtps::ChangeKind_t::ALIVE, buf, msg.getTotalSize() + 4, request_identify));
      // for service通信
      // sequenceNumberを取得して、別の変数にセット

      // promise
      // std::promise<uint8_t *> promise;
      // auto future = promise.get_future();
      // {
      //   std::lock_guard<std::mutex> lock(map_mutex);
      //   promise_map[sequenceNumber_pub.low] = std::move(promise);
      // }
      // return future;
    }
  }

  /*
   *  Subscriber functions
   */
  // typedef struct
  // {
  //   void (*cb_fp)(intptr_t);
  //   intptr_t argp;
  // } SubscribeDataType;

  template <class T>
  Subscriber Node::create_subscription(std::string topic_name, int qos, void (*fp)(T *))
  {
    rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rt/" + topic_name).c_str(), message_traits::TypeName<T *>().value(), false);
    if (reader == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create reader in create_subscription()");
      while (true)
      {
      }
    }

    Subscriber sub;
    sub.topic_name = topic_name;
    sub.cb_fp = (void (*)(intptr_t))fp;

    SubscribeDataType *data_p;
    data_p = new SubscribeDataType;
    data_p->cb_fp = (void (*)(intptr_t))fp;
    data_p->argp = (intptr_t)NULL;
    data_p->callback_worker.reset(
        new LatestMessageCallbackWorker<T>(data_p->cb_fp));
    reader->registerCallback(sub.callback_handler<T>, (void *)data_p);

    /* Register callback to ensure that a subscriber is matched to the reader before receiving messages */
    part_ptr->registerOnNewPublisherMatchedCallback(subMatch, &pubMatched);

    MROS2_DEBUG("[MROS2LIB] create_subscription complete.");
    return sub;
  }

  template <class T, class U>
  void Subscriber::service_callback_handler(void *callee, const rtps::ReaderCacheChange &cacheChange)
  {
    T msg;
    U response_msg;
    const uint8_t *cacheData = cacheChange.getData(); // dataのpointerを取得
    msg.copyFromBuf(&cacheData[4]);                   // copyFromBufはメッセージクラスにある関数

    // for service通信
    const uint32_t response = cacheChange.response;
    const uint32_t response_id = cacheChange.sn.low;

    // for service server test
    const uint32_t service_msg_sn_high = cacheChange.identify.sn.high;
    const uint32_t service_msg_sn_low = cacheChange.identify.sn.low;
    // const uint8_t writer_id = cacheChange.identify.writerId.entityKind;
    const std::array<uint8_t, 3> entityKey = cacheChange.identify.writerId.entityKey; // writer_id: 0003 0100  -> 0001 0300
    // const uint32_t writer_id = 1;
    // MROS2_DEBUG("[MROS2LIB] service response get [callback_handler] entityKey: %lx entityKey: %lx entityKey: %lx entityKey: %lx service_msg_sn_high: %lx service_msg_sn_low: %lx", entityKey, entityKey[0], entityKey[1], entityKey[2], service_msg_sn_high, service_msg_sn_low);

    request_identify.guidPrefix = cacheChange.writerGuid.prefix;
    request_identify.writerId = cacheChange.writerGuid.entityId;
    request_identify.sn.high = cacheChange.sn.high;
    request_identify.sn.low = cacheChange.sn.low;

    request_identify_queue.push(request_identify);

    SubscribeDataType *sub = (SubscribeDataType *)callee;
    void (*fp)(intptr_t, intptr_t) = sub->cb_fp_srv;
    // fp((intptr_t)&msg, (intptr_t)response_msg);
    fp((intptr_t)&msg, (intptr_t)&response_msg);

    // response_msgに値が入ってるか確認
    MROS2_DEBUG("[MROS2LIB] response_msg: %d", response_msg.data);

    // 計算内容を送信
    // メッセージの初期化
    auto func = [&response_msg]
    {
      rtps::DataSize_t len = 0;
      rtps::DataSize_t mod_len = 0;
      size_t cdr_enc_offset = 0;

      if (0 == response_msg.getPubCnt())
      {
        cdr_enc_offset = 4;
        frag_buf[0] = 0;
        frag_buf[1] = 1;
        frag_buf[2] = 0;
        frag_buf[3] = 0;
      }

      auto ret = response_msg.copyToFragBuf(&frag_buf[cdr_enc_offset],
                                            sizeof(frag_buf) - cdr_enc_offset);
      len = ret.second + cdr_enc_offset;
      if (ret.first)
      {
        if (0 < ret.second)
        {
          mod_len = len % 4;
          if (mod_len > 0)
          {
            for (int i = 0; i < (4 - mod_len); i++)
            {
              frag_buf[len++] = 0;
            }
          }
        }
        else
        {
          response_msg.resetCount();
        }
      }
      return std::make_pair(frag_buf, (rtps::DataSize_t)(len));
    };

    if (sizeof(buf) < response_msg.calcTotalSize())
    {
      // pub_ptr->newChangeCallback(rtps::ChangeKind_t::ALIVE,
      //                            func, response_msg.calcTotalSize());
      MROS2_DEBUG("[MROS2LIB] OVER SIZE response_msg.calcTotalSize() > sizeof(buf)");
    }
    else
    {
      response_msg.copyToBuf(&buf[4]);
      response_msg.memAlign(&buf[4]);

      if (!request_identify_queue.empty())
      {
        rtps::Sample_Indetify identify = request_identify_queue.front();
        // front_sampleで処理を行う
        request_identify_queue.pop(); // 先頭要素を削除
        // rtps::CacheChange *result = const_cast<rtps::CacheChange *>(pub_ptr->newChangeIdentify(rtps::ChangeKind_t::ALIVE, buf, response_msg.getTotalSize() + 4, identify));
        rtps::CacheChange *result = const_cast<rtps::CacheChange *>(sub->pub_ptr_own->newChangeIdentify(rtps::ChangeKind_t::ALIVE, buf, response_msg.getTotalSize() + 4, identify));
        SequenceNumber_t sequenceNumber_pub = result->sequenceNumber;
        MROS2_DEBUG("[MROS2LIB] sequenceNumber_pub: %d", sequenceNumber_pub.low);
      }
    }
  }

  // intptr_t *msg_buffer;
  template <class T>
  void Subscriber::callback_handler(void *callee, const rtps::ReaderCacheChange &cacheChange)
  {
    const uint8_t *cacheData = cacheChange.getData(); // dataのpointerを取得

    // // for service communication
    // CacheChangeInfo info;
    // info.writerGuid = cacheChange.writerGuid;
    // info.sequenceNumber = cacheChange.sn;

    // {
    //   std::lock_guard<std::mutex> lock(bufferMutex);
    //   cacheChangeQueue.push(info);
    // }

    // for service通信
    const uint32_t response = cacheChange.response;
    // ROS 2 services correlate a reply through PID_RELATED_SAMPLE_IDENTITY,
    // not through the server writer's independent RTPS sequence number.
    const uint32_t response_id = cacheChange.identify.sn.low;
    const uint32_t response_sequence_number = cacheChange.sn.low;

    MROS2_DEBUG("[MROS2LIB] message get [callback_handler] msg_sn_high: %lx msg_sn_low: %lx", cacheChange.sn.high, cacheChange.sn.low);

    if (response != 0)
    {
      // resposeがある場合は、サービスレスポンスが受信された場合
      MROS2_DEBUG("[MROS2LIB] service response get [callback_handler] request_id=%u response_sn=%u",
                  response_id, response_sequence_number);
      // serviceが来たことを通知
      // cv.notify_one();
      std::promise<std::vector<uint8_t>> promise;
      {
        std::lock_guard<std::mutex> lock(map_mutex);
        auto it = promise_map.find(response_id);
        if (it != promise_map.end())
        {
          promise = std::move(it->second);
          promise_map.erase(it);
        }
        else
        {
          // 要求送信直後の応答は、async_send_request() がpromiseを登録
          // する前に到着し得る。ReaderCacheChangeの寿命外へ持ち出さない
          // よう、ここで所有コピーを保存する。
          const uint8_t *response_data = cacheChange.getData();
          if (early_service_responses.size() < kMaxEarlyServiceResponses)
          {
            early_service_responses[response_id] = std::vector<uint8_t>(
                response_data, response_data + cacheChange.getDataSize());
            MROS2_DEBUG("[MROS2LIB] cached early service response: id=%u",
                        response_id);
          }
          else
          {
            MROS2_DEBUG("[MROS2LIB] dropped unmatched service response: id=%u",
                        response_id);
          }
          return;
        }
      }
      // ReaderCacheChange はcallback復帰後に再利用され得るため、借用
      // ポインタではなくRTPS frameの所有コピーをサービス呼び出し側へ渡す。
      const uint8_t *response_data = cacheChange.getData();
      promise.set_value(std::vector<uint8_t>(
          response_data, response_data + cacheChange.getDataSize()));

      // create_client() はReply frameをfutureへ渡すだけでよい。Tはrequest型
      // なので、下のsubscription workerへ流すとresponseをrequestとして
      // 誤ってデシリアライズしてしまう。
      SubscribeDataType *sub = (SubscribeDataType *)callee;
      if (sub->service_response_only)
      {
        return;
      }
    }

    SubscribeDataType *sub = (SubscribeDataType *)callee;
    if (sub->callback_worker == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] subscription callback worker is not initialized");
      return;
    }

    // Do not invoke the user callback on the DDS receive thread.  enqueue()
    // copies the frame synchronously, then the fixed worker deserializes and
    // calls the existing callback ABI with a T* encoded as intptr_t.
    sub->callback_worker->enqueue(cacheData, cacheChange.getDataSize());
  }

  /*
   *  Other utility functions
   */
  void spin()
  {
    while (true)
    {
#ifndef __MBED__
      osStatus_t ret;
      SubscribeDataType *msg;
      ret = osMessageQueueGet(subscriber_msg_queue_id, &msg, NULL, osWaitForever);
      if (ret != osOK)
      {
        MROS2_ERROR("[MROS2LIB] ERROR: mROS2 spin() wait error %d", ret);
      }
#else  /* __MBED__ */
      // The queue above seems not to be pushed anywhere. So just sleep.
      ThisThread::sleep_for(1000);
#endif /* __MBED__ */
    }
  }
} /* namespace mros2 */

/*
 *  Declaration for embeddedRTPS participants
 */
void *
    networkSubDriverPtr;
void *networkPubDriverPtr;
void (*hbPubFuncPtr)(void *);
void (*hbSubFuncPtr)(void *);

extern "C" void callHbPubFunc(void *arg)
{
  if (hbPubFuncPtr != NULL && networkPubDriverPtr != NULL)
  {
    (*hbPubFuncPtr)(networkPubDriverPtr);
  }
}
extern "C" void callHbSubFunc(void *arg)
{
  if (hbSubFuncPtr != NULL && networkSubDriverPtr != NULL)
  {
    (*hbSubFuncPtr)(networkSubDriverPtr);
  }
}

void setTrue(void *args)
{
  *static_cast<volatile bool *>(args) = true;
}

/*
 * specialize template functions described in platform's workspace
 */
#include "templates.hpp"

// Service instantiations are optional; topic-only apps may not provide this header.
#if __has_include("templates-service.hpp")
#include "templates-service.hpp"
#endif
