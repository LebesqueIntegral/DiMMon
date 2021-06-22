/*
 * Copyright (c) 2013-2018 Konstantin Stefanov
 */

/*
 * Receive data over IP module
 * XXX Works only with dgram sockets
 */

#include <errno.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "dmm_base.h"
#include "dmm_log.h"
#include "dmm_message.h"
#include "dmm_sockevent.h"

#include "recv.h"
#include "common_impl.h"

#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>
#include<unistd.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netdb.h>
#include<arpa/inet.h>
#include<infiniband/arch.h>
#include<infiniband/verbs.h>
#include<rdma/rdma_cma.h>
#include<fcntl.h>
#include<sys/poll.h>
#include <errno.h>

#define ERRORINT(out, y) if ((out)) {int err = errno; printf((y)); printf("Error: %s\n", strerror(err)); exit(1);}
#define ERRORNULL(out, y) if ((out) == NULL) {printf((y)); exit(1);}
#define N 10
#define TIMEOUT 10
#define BUFSIZE 1001

/* Default receive buffer length */
#define DMM_NETIPRECV_DEFAULTBUFLEN 65507

using namespace std;

class ClientInfo
{
public:
	struct rdma_cm_id* cm_id;
	struct ibv_pd* pd;
	struct ibv_comp_channel* comp_chan;
	struct ibv_cq* cq;
	uint32_t* buf;
	struct ibv_mr* mr;
	struct ibv_ah* ah;
	uint32_t remote_qpn;
	uint32_t remote_qkey;

	ClientInfo() {}
	~ClientInfo()
	{
		ERRORINT(ibv_destroy_ah(ah), "Couldn't destroy ah\n");
		rdma_destroy_qp(cm_id);
		ERRORINT(ibv_destroy_cq(cq), "Couldn't destroy cq\n");
		ERRORINT(ibv_destroy_comp_channel(comp_chan), "Couldn't destroy comp_chan\n");
		ERRORINT(ibv_dereg_mr(mr), "Couldn't dereg mr\n");
		free(buf);
		ERRORINT(ibv_dealloc_pd(pd), "Couldn't dealloc pd\n");
		ERRORINT(rdma_destroy_id(cm_id), "Couldn't destroy cm_id\n");
	}

};

class Server
{
private:
	struct rdma_event_channel* cm_channel;
	struct rdma_cm_id* listen_id;
	ClientInfo** list;
	int clientCount;
	char* result;

public:
	Server(char* port)
	{
		result = new char[BUFSIZE * 4 - 4];
		clientCount = 0;
		list = new ClientInfo * [N];
		for (int i = 0; i < N; i++)
			list[i] = NULL;
		struct rdma_addrinfo* res;
		struct rdma_addrinfo hints;

		//Настраиваем параметры для получения сокета
		memset(&hints, 0, sizeof(hints));

		hints.ai_flags = RAI_PASSIVE;
		hints.ai_family = AF_INET;
		hints.ai_port_space = RDMA_PS_UDP;

		// Создание RDMA CM структур

		ERRORNULL(cm_channel = rdma_create_event_channel(), "Couldn't do rdma event channel\n");
		ERRORINT(rdma_create_id(cm_channel, &listen_id, NULL, RDMA_PS_UDP), "Counldn't create rdma id\n");
		ERRORINT(rdma_getaddrinfo(NULL, port, &hints, &res), "Cannot getaddr\n");
		ERRORINT(rdma_bind_addr(listen_id, res->ai_src_addr), "Couldn't bind to my addr\n");
		ERRORINT(rdma_listen(listen_id, 1), "Couldn't do rdma listen\n");

		int flags = fcntl(cm_channel->fd, F_GETFL);
		ERRORINT(fcntl(cm_channel->fd, F_SETFL, flags | O_NONBLOCK) < 0,
			"Failed to change file descriptor\n");
		rdma_freeaddrinfo(res);
		printf("Server created\n");
	}

	int GetFd()
	{
		return cm_channel->fd;
	}
	
	void Run()
	{
		int count = TIMEOUT; //Количество секунд, когда пользователь неподключается
		while (count)
		{
			//printf("Client count: %d\n", clientCount);
			while (CheckEventChannel() != -1);
			while (ProcessResponse(true) != -1);
			/*if (clientCount == 0)
				count--;
			else
				count = 10;
			sleep(1);
		*/}
	}
	~Server()
	{
		delete[] result;
		ERRORINT(rdma_destroy_id(listen_id), "Couldn't destroy listen_id\n");
		rdma_destroy_event_channel(cm_channel);
		for (int i = 0; i < clientCount; i++)
			delete list[i];
		delete list;
	}
	
	int ProcessResponse(bool receiveOnly = false, int clientNumber = -1) //Для обработки определенного клиента (clientNumber) или всех (при clientNumber = -1); (Возвращает 1 - если кого-то обработали или -1 - некому отвечать)
	{
		struct ibv_wc wc;
		void *cq_context;
		
		int start = clientNumber == -1? 0: clientNumber;
		int finish = clientNumber == -1? clientCount: (clientNumber + 1);
		for (clientNumber = start; clientNumber < finish; clientNumber++)
		{	
			ClientInfo& c = *(list[clientNumber]);
			//if (ibv_poll_cq(c.cq, 1, &wc) < 1 || wc.status != IBV_WC_SUCCESS)

			if (ibv_get_cq_event(c.comp_chan, &c.cq, &cq_context) == -1)
				continue;
			
			ibv_poll_cq(c.cq, 1, &wc);
			ibv_ack_cq_events(c.cq, 1);
			ERRORINT(ibv_req_notify_cq(c.cq, 0), "Couldn't req notify\n");
			
			CreateReplyAh(c, wc);
			
			Unpack(c.buf + 10, result, wc.byte_len - 40); 

			Factory(result, wc.byte_len - 40);//считаем, что Factory не изменяет result	
			
			if (receiveOnly == true)
			{
				if (wc.byte_len <= 40)//только заголовок
					DeleteClient(clientNumber);
				else
					CreatePostRecieve(c);
				return 1;
			}

			
			if (wc.byte_len > 40)//не только заголовок
			{
				Pack(result, c.buf, wc.byte_len - 40);
				CreatePostRecieve(c);
				CreatePostSend(c, wc.byte_len - 40);
			}
			else
				DeleteClient(clientNumber);
			return 1;
		}
		return -1;//nobody was proccessed
	}	
	int CheckEventChannel() //Возвращает fd от cq - которого добавил или -1, если никто не стучится в event_channelч
	{
		struct rdma_cm_event* event;
		int fd_comp_channel = -1;
		if (!rdma_get_cm_event(cm_channel, &event)) //если есть что-то в очереди
			{
				if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST)
					printf("Я получил не RDMA_CM_EVENT_CONNECT_REQUEST\n");
				else
					fd_comp_channel = AddNewClient(event);
				rdma_ack_cm_event(event);
			}
		//printf("Server: CheckEventChannel fd = %d\n", fd_comp_channel);
		return fd_comp_channel;
	}
	

protected: virtual void Factory(char* result, uint32_t byte_len) = 0; //переписывает result, это потом и отправим в ProcessResponse(), если не стоит флаг recieveOnly; byte_len - размер result

private:
	void DeleteClient(int i)
	{
		delete list[i];
		clientCount--;
		for (; i < clientCount; i++)
			list[i] = list[i + 1];
	}
	void CreateReplyAh(ClientInfo& c, struct ibv_wc& wc)
	{
		struct ibv_qp_attr attr;
		struct ibv_qp_init_attr init_attr;

		if (c.ah != NULL)
			return;

		c.ah = ibv_create_ah_from_wc(c.pd, &wc, (ibv_grh*)c.buf, c.cm_id->port_num);
		c.remote_qpn = be32toh(wc.imm_data);
		ibv_query_qp(c.cm_id->qp, &attr, IBV_QP_QKEY, &init_attr);
		c.remote_qkey = attr.qkey;
	}
	
	int AddNewClient(struct rdma_cm_event* event)
	{
		ClientInfo* c = new ClientInfo();
		c->ah = NULL;
		c->cm_id = event->id;
		ERRORNULL(c->pd = ibv_alloc_pd(c->cm_id->verbs), "Couldn't alloc pd\n");
		ERRORNULL(c->comp_chan = ibv_create_comp_channel(c->cm_id->verbs), "Couldn't create comp channel\n");
		int flags = fcntl(c->comp_chan->fd, F_GETFL);
		ERRORINT(fcntl(c->comp_chan->fd, F_SETFL, flags | O_NONBLOCK) < 0,
			"Failed to change file descriptorof completion event channel\n");
		
		ERRORNULL(c->cq = ibv_create_cq(c->cm_id->verbs, 10, c->cm_id, c->comp_chan, 0), "Couldn't create cq\n");
		ERRORINT(ibv_req_notify_cq(c->cq, 0), "Couldn't req notify\n");
		
		c->buf = (uint32_t*)calloc(1, BUFSIZE * sizeof(uint32_t) + sizeof(struct ibv_grh));
		ERRORNULL(c->mr = ibv_reg_mr(c->pd, c->buf, BUFSIZE * sizeof(uint32_t) + sizeof(struct ibv_grh), IBV_ACCESS_LOCAL_WRITE), "Couldn't red mr\n");

		struct ibv_qp_init_attr qp_attr = {};
		qp_attr.cap.max_send_wr = 1;
		qp_attr.cap.max_send_sge = 1;
		qp_attr.cap.max_recv_wr = 2;
		qp_attr.cap.max_recv_sge = 1;
		qp_attr.send_cq = c->cq;
		qp_attr.recv_cq = c->cq;
		qp_attr.qp_type = IBV_QPT_UD;

		ERRORINT(rdma_create_qp(c->cm_id, c->pd, &qp_attr), "Couldn't create rdma qp\n");
		list[clientCount] = c;
		clientCount++;

		CreatePostRecieve(*c);
		//Не забыть проверить успешность работы по получению данных
		struct rdma_conn_param conn_param = {};
		memset(&conn_param, 0, sizeof(conn_param));
		ERRORINT(rdma_accept(c->cm_id, &conn_param), "Couldn't accept\n");
		printf("New Client. Id: %d\n", *(c->cm_id));
		return c->cq->channel->fd;
	}
	void CreatePostRecieve(ClientInfo& c)
	{
		struct ibv_wc wc;
		struct ibv_sge sge;
		struct ibv_recv_wr recv_wr = {};
		struct ibv_recv_wr* bad_recv_wr;
		void *cq_context;

		sge.addr = (uintptr_t)c.buf;
		sge.length = BUFSIZE * sizeof(uint32_t) + sizeof(struct ibv_grh);
		sge.lkey = c.mr->lkey;

		recv_wr.next = NULL;
		recv_wr.wr_id = 0;
		recv_wr.sg_list = &sge;
		recv_wr.num_sge = 1;

		if (ibv_post_recv(c.cm_id->qp, &recv_wr, &bad_recv_wr) == -1)
			printf("Errno: %d. Couldn't do post recv\n", errno);
	}
	void CreatePostSend(ClientInfo& c, int len)
	{
		struct ibv_wc wc;
		struct ibv_sge sge;
		struct ibv_send_wr send_wr = {};
		struct ibv_send_wr* bad_send_wr;
		void *cq_context;
		
		sge.addr = (uintptr_t)c.buf;
		sge.length = len; //in bytes
		sge.lkey = c.mr->lkey;

		send_wr.next = NULL;
		send_wr.wr_id = 1;
		send_wr.opcode = IBV_WR_SEND_WITH_IMM;
		send_wr.send_flags = IBV_SEND_SIGNALED;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;

		send_wr.wr.ud.ah = c.ah;
		send_wr.wr.ud.remote_qpn = c.remote_qpn;
		send_wr.wr.ud.remote_qkey = c.remote_qkey;

		
		if (ibv_post_send(c.cm_id->qp, &send_wr, &bad_send_wr) == -1)
			printf("Errno: %d. Couldn't do post send after Communication\n", errno);
		
		//while (ibv_poll_cq(c.cq, 1, &wc) == 0);
		while (ibv_get_cq_event(c.comp_chan, &c.cq, &cq_context) == -1);
		
		ibv_poll_cq(c.cq, 1, &wc);
		ibv_ack_cq_events(c.cq, 1);
		ERRORINT(ibv_req_notify_cq(c.cq, 0), "Couldn't req notify\n");
		ERRORINT(wc.wr_id != 1 || wc.status != IBV_WC_SUCCESS, "WC NOT_SUCCESS\n");
	}
	
	int Pack(const char* input, uint32_t* output, int len)//pack char* to int* before send (get input len) -> return char number == bytes(output len) for PostSend
	{
		if (input == NULL)//if client want to disconnect
		{
			//output[0] = 0;
			return 0;
		}
		
		int i = 0;
		for ( ; i < len; i ++)
			output[i / 4] = (output[i / 4] << 8) | input[i];
		//теперь выровнять нулями до кратности 4
		len = (len - 1) / 4 * 4 + 4; //len кратно 4 (до скольки байт надо дозабить 0)
		for ( ; i < len; i++)
			output[i / 4] = output[i / 4] << 8;
		return len; 
	}

	void Unpack(const uint32_t* input, char* output, int len) //unpack int* to char* after receive (get len in bytes)
	{
		for (int i = 0; i < len; i++)
			output[i] = input[i / 4] >> (24 - i % 4 * 8);
		output[len] = 0; //need for string data
	}
};

struct DmmServer : Server
{
	dmm_hook_p outhook;
	dmm_node_p node;
	
	DmmServer(char* port, dmm_node_p n) : Server(port)
	{
		outhook = NULL;
		node = n;		
	}
	protected:
	void Factory (char* result, uint32_t byte_len)
	{
		dmm_data_p      data;
		dmm_datanode_p  dn;
		dmm_size_t 		bytes_recvd = byte_len;
		if (outhook != NULL) 
		{
			if ((data = DMM_DATA_CREATE_RAW(0, bytes_recvd)) == NULL) 
			{
				dmm_log(DMM_LOG_ERR,
						"Node " DMM_PRINODE ": can't allocate memory for data",
						DMM_NODEINFO(node)
					   );
				//return ENOMEM;
			}
	
			dn = DMM_DATA_NODES(data); 

			memcpy(dn, result, bytes_recvd); //в dn кладем те самые данные, которые прочитали из сокета в recvmsg
			
			//printf("Server: byte_len = %d\n", byte_len);
			DMM_DATA_SEND(data, outhook); //говорим Диммону отправить данные на такой-то хук
			DMM_DATA_UNREF(data);
		
		}
	}
};

struct pvt_data {
    int         fd; //rdma_event_channel
	DmmServer* server;
    dmm_hook_p  outhook;
    void       *buf;
    size_t      buflen;
    uint32_t    flags;
};

static uint32_t last_token = 0;

#define GET_TOKEN() (++last_token)

static bool check_data_valid(void *data, size_t len)
{
    dmm_datanode_p dn;
    ssize_t slen;

    if (len < sizeof(struct dmm_datanode)) {
        dmm_log(DMM_LOG_WARN, "Received short message");
        return false;
    }
    for (dn = (dmm_datanode_p)data, slen = len; !DMM_DN_ISEND(dn) && len > 0; DMM_DN_ADVANCE(dn))
        slen -= sizeof(struct dmm_datanode) + dn->dn_len;

     if (slen <= 0) {
         dmm_log(DMM_LOG_WARN, "Received message: bad data structure");
         return false;
     }
     return true;
}

static int process_bind_msg(dmm_node_p node, dmm_msg_p msg) //формирует подписку - за чем следить (fd), где (node) и по какому случаю дергать событие (приход данных - DMM_SOCKEVENT_IN)
{
    struct dmm_msg_netiprecv_bind *nb;
    struct pvt_data *pvt;
    dmm_msg_p ses; // ses is socket event subscribe message
    int err;
	const char *p;
	char port[DMM_NETIP_MAXADDRLEN];
	
    assert (msg->cm_type == DMM_MSGTYPE_NETIPRECV && msg->cm_cmd == DMM_MSG_NETIPRECV_BIND); //проверка на сбой

    pvt = (struct pvt_data *)DMM_NODE_PRIVATE(node); //достали тот самый указатель pvt из node
    nb = DMM_MSG_DATA(msg, struct dmm_msg_netiprecv_bind); //хотим в nb положить из msg структуру указатель типа dmm_msg_netiprecv_bind, Диммон прислал msg

	//Достаем порт
    p = strrchr(nb->addr, ':');
    if (p == NULL)
        return EINVAL;
    //Скопируем в port значение после символа ":"
    strcpy(port, p + 1);
	printf("%s\n", port);
	pvt->server = new DmmServer(port, node); //в конструкторе происходит bind
	pvt->fd = pvt->server->GetFd();
	
	ses = DMM_MSG_CREATE(DMM_NODE_ID(node), //эта нода - заказчик
                         DMM_MSG_SOCKEVENTSUBSCRIBE, //подписывается на получение событий
                         DMM_MSGTYPE_GENERIC, //такого типа
                         GET_TOKEN(), //похоже предикат - что вернет событие - 1 или 0 например
                         0,
                         sizeof(struct dmm_msg_sockeventsubscribe)
                        ); //создали ses - подписка сокета на события типа dmm_msg_sockeventsubscribe
    if (ses == NULL) {
        err = ENOMEM;
        goto finish;
    } //если не смогли подписку сделать, она будет Null - ошибка
    DMM_MSG_DATA(ses, struct dmm_msg_sockeventsubscribe)->fd = pvt->fd; //достаем из ses fd и присваиваем ему сокет этой ноды
    DMM_MSG_DATA(ses, struct dmm_msg_sockeventsubscribe)->events = DMM_SOCKEVENT_IN; //событие - когда на этот сокет (119 строчка) приходят данные (событие) 
    err = DMM_MSG_SEND_ID(DMM_NODE_ID(node), ses); //отправляем Диммону нашу подписку на событие прихода данных на сокет этой ноды

finish:
    return err;
}

static int process_socket_event(dmm_node_p node, uint32_t events) //что-то пришло на наш fd - данные
{
    struct pvt_data *pvt;
	dmm_msg_p ses; // ses is socket event subscribe message
	int fd; //чтобы сделать подписку на rdma_event_chanel в первый раз

    pvt = (struct pvt_data *)DMM_NODE_PRIVATE(node); //достаем из node указатель на ее pvt 

    if ((events & ~DMM_SOCKEVENT_IN) != 0) { //проверка на ошибку - похоже, что если нам пришло событие типа DMM_SOCKEVENT_OUT, мы такие не умеем обрабатывать, говорим об этом Диммону
        /* Not a read event on read socket */
        dmm_log(DMM_LOG_WARN,
                "Node " DMM_PRINODE ": Received socket event is not DMM_SOCKEVENT_IN for fd %d",
                DMM_NODEINFO(node), pvt->fd
               );
        return EINVAL;
    }
    
	if ( (fd = pvt->server->CheckEventChannel()) > 0)//1 - добавили нового клиента
	{
		ses = DMM_MSG_CREATE(DMM_NODE_ID(node), //эта нода - заказчик
                         DMM_MSG_SOCKEVENTSUBSCRIBE, //подписывается на получение событий
                         DMM_MSGTYPE_GENERIC, //такого типа
                         GET_TOKEN(), //похоже предикат - что вернет событие - 1 или 0 например
                         0,
                         sizeof(struct dmm_msg_sockeventsubscribe)
                        ); //создали ses - подписка сокета на события типа dmm_msg_sockeventsubscribe
		if (ses == NULL) 
			return ENOMEM;	
		
		DMM_MSG_DATA(ses, struct dmm_msg_sockeventsubscribe)->fd = fd; //достаем из ses fd и присваиваем ему сокет этой ноды
		DMM_MSG_DATA(ses, struct dmm_msg_sockeventsubscribe)->events = DMM_SOCKEVENT_IN; //событие - когда на этот сокет (119 строчка) приходят данные (событие) 
		//printf("Client added with fd (from cq) = %d\n", fd);
		return DMM_MSG_SEND_ID(DMM_NODE_ID(node), ses); //отправляем Диммону нашу подписку на событие прихода данных на сокет этой ноды
	}
	
	pvt->server->ProcessResponse(true);//true means receiveOnly

    return 0;
}

static int recv_ctor(dmm_node_p node)
{
    struct pvt_data *pvt;

    dmm_debug("Constructor called for " DMM_PRINODE, DMM_NODEINFO(node));
    if ((pvt = (struct pvt_data *)DMM_MALLOC(sizeof(*pvt))) == NULL) {
        dmm_log(DMM_LOG_ERR, "Cannot allocate memory for private info");
        return ENOMEM;
    }
    pvt->buflen = DMM_NETIPRECV_DEFAULTBUFLEN;
    if ((pvt->buf = DMM_MALLOC(pvt->buflen)) == NULL) {
        dmm_log(DMM_LOG_ERR, "Cannot allocate memory for receive buffer");
        DMM_FREE(pvt);
        return ENOMEM;
    }

    pvt->fd = -1;
	pvt->server = NULL;
    pvt->outhook = NULL;
    pvt->flags = 0;
    DMM_NODE_SETPRIVATE(node, pvt); //Сделать переменную приватной, чтобы нельзя было достучаться извне - 
    return 0;
}

static void recv_dtor(dmm_node_p node)
{
	struct pvt_data *pvt;
    pvt = (struct pvt_data *)DMM_NODE_PRIVATE(node); //достаем из node указатель на ее pvt 
	
	if (pvt->server != NULL)
		delete pvt->server;
	pvt->server = NULL;
    DMM_FREE(DMM_NODE_PRIVATE(node));
}

static int recv_newhook(dmm_hook_p hook)
{
    struct pvt_data *pvt;

    pvt = (struct pvt_data *)DMM_NODE_PRIVATE(DMM_HOOK_NODE(hook));

    if (DMM_HOOK_ISIN(hook))
        return EINVAL;
    if (pvt->outhook != NULL)
        return EEXIST;

    pvt->outhook = hook;
	pvt->server->outhook = hook;

    return 0;
}

static void recv_rmhook(dmm_hook_p hook)
{
    struct pvt_data *pvt;

    pvt = (struct pvt_data *)DMM_NODE_PRIVATE(DMM_HOOK_NODE(hook));
    assert(hook == pvt->outhook);
    pvt->outhook = NULL;
}



//////////////////////////////////////////////ОСНОВНАЯ ТОЧКА УПРАВЛЕНИЯ/////////////////////////////////////////
static int recv_rcvmsg(dmm_node_p node, dmm_msg_p msg)
{
    struct pvt_data *pvt;
    dmm_msg_p resp;
    int err = 0;

    if (msg->cm_flags & DMM_MSG_RESP) //Выцепляем из флагов, поднят ли флаг DMM_MSG_RESP - если да, то нам пришел ответ и мы игнорируем на финише.
        goto finish;

    pvt = (struct pvt_data *)DMM_NODE_PRIVATE(node); //DMM_NODE_PRIVATE - возвращает pvt, созданную из конструктора
    switch (msg->cm_type) {  //GENERIC, NETIPRECV и DEFAULT
    case DMM_MSGTYPE_GENERIC:
        switch (msg->cm_cmd) {
        case DMM_MSG_SOCKEVENTTRIGGER: { //Что-то пришло в rdma_event_channel?
            struct dmm_msg_sockeventtrigger *se;
            se = DMM_MSG_DATA(msg, struct dmm_msg_sockeventtrigger); //DMM_MSG_DATA - выцепить указатель указанного типа (struct dmm_msg_sockeventtrigger) из msg
           /* if (pvt->fd != se->fd) { //Проверка на ошибку - что действительно на эту ноду пришло сообщение 
                dmm_log(DMM_LOG_WARN,
                        "Node " DMM_PRINODE ": Received socket event for fd %d, "
                        "our fd is %d",
                        DMM_NODEINFO(node), se->fd, pvt->fd
                       );
                err = EINVAL;
                break;
            }//Если ошибки не было, то обрабатываем это событие на том самом node
            */err = process_socket_event(node, se->events);
            break;
        }

        default:
            err = ENOTSUP; //Не знаем, что делать в других случаях, ENOTSUP - допишите код
            break;
        }
        break;

    case DMM_MSGTYPE_NETIPRECV:

#define CREATE_SEND_EMPTY_RESP()                                    \
        do {                                                        \
            resp = DMM_MSG_CREATE_RESP(DMM_NODE_ID(node), msg, 0);  \
            if (resp != NULL) {                                     \
                if (err != 0)                                       \
                    msg->cm_flags |= DMM_MSG_ERR;                   \
                                                                    \
                DMM_MSG_SEND_ID(msg->cm_src, resp);                 \
            } else                                                  \
                err = (err != 0) ? err : ENOMEM;                    \
        } while (0)

        switch (msg->cm_cmd) {
        case DMM_MSG_NETIPRECV_CREATESOCK: //Создаем сокет и кладем во флаг, что сокет создан. Мы это не будем обрабатывать, сразу в bind будем создавать
            /*err = process_createsock_msg(node, msg);
            if (err == 0)
                pvt->flags |= DMM_NETIPRECV_HASSOCK;
            */
			CREATE_SEND_EMPTY_RESP();
            break;

        case DMM_MSG_NETIPRECV_BIND: //Биндимся и ставим флаг
            err = process_bind_msg(node, msg);
            if (err == 0)
			{
				pvt->flags |= DMM_NETIPRECV_HASSOCK;
                pvt->flags |= DMM_NETIPRECV_BOUND;
			}
			CREATE_SEND_EMPTY_RESP();
            break;

        case DMM_MSG_NETIPRECV_GETFLAGS: //DiMMon хочет знать, какие флаги лежат в этой node
            assert(msg->cm_len == 0);

            resp = DMM_MSG_CREATE_RESP(DMM_NODE_ID(node), msg, sizeof(struct dmm_msg_netiprecv_getflags_resp));
            if (resp != NULL) {
                DMM_MSG_DATA(resp, struct dmm_msg_netiprecv_getflags_resp)->flags = pvt->flags;
                DMM_MSG_SEND_ID(msg->cm_src, resp);
            } else
                err = ENOMEM;
            break;

        case DMM_MSG_NETIPRECV_SETFLAGS: {//DiMMon хочет принудительно установить флаги в node
            int flags;
            assert(msg->cm_len == sizeof(struct dmm_msg_netiprecv_setflags));

            flags = DMM_MSG_DATA(msg, struct dmm_msg_netiprecv_setflags)->flags;
            if ((flags & ~DMM_NETIPRECV_SETTABLEFLAGS) != 0)
                err = EINVAL;
            else {
                /* Auxiliary flags should be kept */
                pvt->flags = (   pvt->flags
                              & ~DMM_NETIPRECV_SETTABLEFLAGS
                             )
                             | DMM_MSG_DATA(msg, struct dmm_msg_netiprecv_setflags)->flags;
            }
            CREATE_SEND_EMPTY_RESP();
            break;
        }

        default:
            err = ENOTSUP;
            break;
        }
#undef CREATE_SEND_EMPTY_RESP

        break;

    default:
        err = ENOTSUP;
        break;
    }

finish:
    DMM_MSG_FREE(msg);
    return err;

}

struct dmm_type recv_type = {
    "net/ip/recv",
    recv_ctor,
    recv_dtor,
    NULL,
    recv_rcvmsg,
    recv_newhook,
    recv_rmhook,
    {},
};
