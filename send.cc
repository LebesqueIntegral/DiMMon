/*
 * Copyright (c) 2013 Konstantin Stefanov
 */

/*
 * Send data over IP module
 * XXX Works only with dgram sockets
 */
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include "dmm_base.h"
#include "dmm_log.h"
#include "dmm_message.h"
#include "send.h"
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
#define BUFSIZE 1001

using namespace std;

enum {
	RESOLVE_TIMEOUT_MS = 5000
};

class Client
{
private:
	struct rdma_event_channel* cm_channel;
	struct rdma_cm_id* cm_id;
	struct ibv_pd* pd;
	struct ibv_comp_channel* comp_chan;
	struct ibv_cq* cq;
	struct ibv_cq* evt_cq;
	struct ibv_mr* mr;
	struct ibv_ah* ah;
	struct ibv_qp_init_attr qp_attr;
	struct rdma_ud_param ud_param;
	struct ibv_wc wc;
	uint32_t* buf;
public:
	Client(char* address, char* port)
	{
		struct rdma_addrinfo* res;
		struct rdma_addrinfo hints;
		struct rdma_cm_event* event;
		struct rdma_conn_param conn_param;
		int err;

		//Настраиваем параметры для получения сокета
		memset(&hints, 0, sizeof(hints));

		hints.ai_family = AF_INET;
		hints.ai_port_space = RDMA_PS_UDP;

		// Создание RDMA CM структур
		ERRORNULL(cm_channel = rdma_create_event_channel(), "Couldn't do rdma event channel\n");
		ERRORINT(rdma_create_id(cm_channel, &cm_id, NULL, (rdma_port_space)hints.ai_port_space), "Counldn't create rdma id\n");
		ERRORINT(rdma_getaddrinfo(address, port, &hints, &res), "Couldn't getaddrinfo\n");

		// Проверка адреса и пути до Server
		while (res)
		{
			err = rdma_resolve_addr(cm_id, NULL, res->ai_dst_addr, RESOLVE_TIMEOUT_MS);
			if (!err)
				break;
			else
				res = res->ai_next;
		}
		ERRORINT(err, "Couldn't resolve address\n");

		ERRORINT(rdma_get_cm_event(cm_channel, &event), "Couldn't get rdma event ADDR_RESOLVED\n");
		if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED)
			exit(1);
		rdma_ack_cm_event(event);

		ERRORINT(rdma_resolve_route(cm_id, RESOLVE_TIMEOUT_MS), "Couldn't resolve route\n");
		ERRORINT(rdma_get_cm_event(cm_channel, &event), "Couldn't get rdma event ROUTE_RESOLVED\n");
		if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
			exit(1);
		rdma_ack_cm_event(event);

		// Создание ibv компонентов для QP, потому что знаем, с кем будет общение

		ERRORNULL(pd = ibv_alloc_pd(cm_id->verbs), "Couldn't alloc pd\n");
		ERRORNULL(comp_chan = ibv_create_comp_channel(cm_id->verbs), "Couldn't create comp channel\n");
		ERRORNULL(cq = ibv_create_cq(cm_id->verbs, 2, NULL, comp_chan, 0), "Couldn't create cq\n");
		ERRORINT(err = ibv_req_notify_cq(cq, 0), "Couldn't req notify\n");
		buf = (uint32_t*)calloc(1, BUFSIZE * sizeof(uint32_t) + sizeof(struct ibv_grh));
		ERRORNULL(mr = ibv_reg_mr(pd, buf, BUFSIZE * sizeof(uint32_t) + sizeof(struct ibv_grh), IBV_ACCESS_LOCAL_WRITE), "Couldn't red mr\n");

		memset(&qp_attr, 0, sizeof(qp_attr));
		
		qp_attr.cap.max_send_wr = 1;
		qp_attr.cap.max_send_sge = 1;
		qp_attr.cap.max_recv_wr = 2;
		qp_attr.cap.max_recv_sge = 1;
		qp_attr.send_cq = cq;
		qp_attr.recv_cq = cq;
		qp_attr.qp_type = IBV_QPT_UD;

		ERRORINT(err = rdma_create_qp(cm_id, pd, &qp_attr), "Couldn't create rdma qp\n");

		conn_param.private_data = res->ai_connect;
		conn_param.private_data_len = res->ai_connect_len;

		//Все компоненты созданы, можно соединяться

		ERRORINT(rdma_connect(cm_id, &conn_param), "Couldn't connect to server\n");
		ERRORINT(rdma_get_cm_event(cm_channel, &event), "Couldn't get event ESTABLISHED\n");
		if (event->event != RDMA_CM_EVENT_ESTABLISHED)
			exit(1);

		memcpy(&ud_param, &(event->param.ud), sizeof(ud_param));
		ah = ibv_create_ah(pd, &event->param.ud.ah_attr);

		rdma_ack_cm_event(event);

		printf("Client created\n");

		rdma_freeaddrinfo(res);
	}
	~Client()
	{
		GetRequest(NULL, NULL, 0);
		ERRORINT(ibv_destroy_ah(ah), "Couldn't destroy ah\n");
		rdma_destroy_qp(cm_id);
		ERRORINT(ibv_destroy_cq(cq), "Couldn't destroy cq\n");
		ERRORINT(ibv_dereg_mr(mr), "Couldn't dereg mr\n");
		free(buf);
		ERRORINT(ibv_destroy_comp_channel(comp_chan), "Couldn't destroy comp_chan\n");
		ERRORINT(ibv_dealloc_pd(pd), "Couldn't dealloc pd\n");
		ERRORINT(rdma_destroy_id(cm_id), "Couldn't destroy cm_id\n");
		rdma_destroy_event_channel(cm_channel);
	}

	void GetRequest(const char* input, char* output, int len) //len = size in bytes; input - откуда послать данные (NULL - отключиться); output - куда положить пришедшие данные (NULL - нам все равно, что пришло)
	{  
		if (output != NULL) //Если мы ждем ответа от сервера
			CreatePostRecieve();
			
		len = Pack(input, buf, len); //int count
		
		CreatePostSend(len);
		//printf("BUF1: %s\n", buf);
		
		if (output != NULL)
		{
			while (ibv_poll_cq(cq, 1, &wc) == 0);
			ERRORINT(wc.status != IBV_WC_SUCCESS, "Wc NOT_SUCCESS\n");
			ERRORINT(wc.wr_id != 0, "I get wc.wr_id != 0\n");
			
			//Unpack(buf + 10, output, wc.byte_len - 40);
		}
		
		//printf("GetRequest Finish\n");

	}


private:
	void CreatePostRecieve()
	{
		struct ibv_sge sge;
		struct ibv_recv_wr recv_wr = {};
		struct ibv_recv_wr* bad_recv_wr;

		sge.addr = (uintptr_t)buf;
		sge.length = BUFSIZE * sizeof(uint32_t) + sizeof(struct ibv_grh);
		sge.lkey = mr->lkey;

		recv_wr.next = NULL;
		recv_wr.wr_id = 0;
		recv_wr.sg_list = &sge;
		recv_wr.num_sge = 1;

		if (ibv_post_recv(cm_id->qp, &recv_wr, &bad_recv_wr) == -1)
			printf("Errno: %d. Couldn't do post recv\n", errno);
	}
	void CreatePostSend(int len)
	{
		struct ibv_sge sge;
		struct ibv_send_wr send_wr = {};
		struct ibv_send_wr* bad_send_wr;

		sge.addr = (uintptr_t)buf;
		sge.length = len; //in bytes
		sge.lkey = mr->lkey;

		send_wr.next = NULL;
		send_wr.wr_id = 1;
		send_wr.opcode = IBV_WR_SEND_WITH_IMM;
		send_wr.send_flags = IBV_SEND_SIGNALED;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.imm_data = htobe32(cm_id->qp->qp_num);

		send_wr.wr.ud.ah = ah;
		send_wr.wr.ud.remote_qpn = ud_param.qp_num;
		send_wr.wr.ud.remote_qkey = ud_param.qkey;

		if (ibv_post_send(cm_id->qp, &send_wr, &bad_send_wr) == -1)
			printf("Errno: %d. Couldn't do post send after Communication\n", errno);

		while (ibv_poll_cq(cq, 1, &wc) == 0);
		ERRORINT(wc.wr_id != 1 || wc.status != IBV_WC_SUCCESS, "Wc NOT_SUCCESS\n");
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

struct pvt_data {
    int         fd;
	Client* client;
    uint32_t    flags;
};

static int connect_socket(dmm_node_p node, int fd, const char *addr)//Аналогичная bind_socket из recv.c
{
	char node_addr[DMM_NETIP_MAXADDRLEN], port[DMM_NETIP_MAXADDRLEN];
    const char *p, *host_start;
    size_t host_len;

	//Парсим Ip-адрес и порт
    p = strrchr(addr, ':');
    if (p == NULL)
        return EINVAL;
    // port is to the right of the rightest colon
    strcpy(port, p + 1);
    if (*(p - 1) == ']') {
        // Случай, если адрес в скобочках
        if (*addr == '[') {
            host_start = addr + 1;
            host_len = p - 1 - host_start;
        } else {
            return EINVAL;
        }
    } else {
        host_start = addr;
        host_len = p - host_start;
    }
    strncpy(node_addr, host_start, host_len);
    node_addr[host_len] = '\0';

	struct pvt_data *pvt;
    pvt = (struct pvt_data *)DMM_NODE_PRIVATE(node); //достаем указатель на pvt этой node
	//Создаем клиента
	pvt->client = new Client(node_addr, port);
	
    return 0;
}

static int send_ctor(dmm_node_p node)
{
    struct pvt_data *pvt;

    dmm_debug("Constructor called for " DMM_PRINODE, DMM_NODEINFO(node));
    if ((pvt = (struct pvt_data *)DMM_MALLOC(sizeof(*pvt))) == NULL) {
        dmm_log(DMM_LOG_ERR, "Cannot allocate memory for private info");
        return ENOMEM;
    }

    pvt->fd = -1;
	pvt->client = NULL;
    pvt->flags = 0;
    DMM_NODE_SETPRIVATE(node, pvt);
    return 0;
}

static void send_dtor(dmm_node_p node)
{
	struct pvt_data *pvt;
    pvt = (struct pvt_data *)DMM_NODE_PRIVATE(node); //достаем указатель на pvt этой node
	if (pvt->client != NULL)
		delete pvt->client;
	pvt->client = NULL;
    DMM_FREE(DMM_NODE_PRIVATE(node));
}

static int send_newhook(dmm_hook_p hook)
{
    if (DMM_HOOK_ISOUT(hook))
        return EINVAL;

    return 0;
}

static int send_rcvdata(dmm_hook_p hook, dmm_data_p data) //Вызывается, когда из входящего хука пришли данные мониторинга
{
    struct pvt_data *pvt;
    int err;
    char errbuf[128], *errmsg;
    dmm_datanode_p dn;
    size_t len;
	char output;
	char input[10] = "Hello\n";

    err = 0;
    pvt = (struct pvt_data *)DMM_NODE_PRIVATE(DMM_HOOK_NODE(hook)); //Получаем указатель на данные, которые лежат во входящем хуке
    if (!(pvt->flags & DMM_NETIPSEND_CONNECTED)) { //Если до этого не были подключены к серверу (из-за сбоя или не было указаний в конфиге), то ошибка
        err = ENOTCONN;
        goto error;
    }
    for (dn = DMM_DATA_NODES(data); !DMM_DN_ISEND(dn); dn = DMM_DN_NEXT(dn))
        ;//пустой for, чтобы прокатиться до конца сообщения, чтобы потом найти разницу между последним и первым байтом
    len = (char *)dn - (char *)DMM_DATA_NODES(data) + sizeof(struct dmm_datanode); //считаем длину сообщения в байтах (в char)
    if (len <= sizeof(struct dmm_datanode)) { //если было пустое сообщение - ошибка 
        dmm_log(DMM_LOG_ERR, "Sending empty messages is not allowed");
        err = EBADMSG;
        goto error;
    }
	
	//printf("Send: Data len = %d\n", len);
	pvt->client->GetRequest(data->da_nodes, NULL, len);
	
error:
    DMM_DATA_UNREF(data);
    return err;
}

static int send_rcvmsg(dmm_node_p node, dmm_msg_p msg)
{
    struct pvt_data *pvt;
    dmm_msg_p resp;
    int err = 0;

    pvt = (struct pvt_data *)DMM_NODE_PRIVATE(node); //достаем указатель на pvt этой node
    switch (msg->cm_type) {
    case DMM_MSGTYPE_NETIPSEND: //хотят, чтобы мы создали сокет
        switch (msg->cm_cmd) {
        case DMM_MSG_NETIPSEND_CREATESOCK:
            /*err = process_createsock_msg(node, msg); //создаем наш клиентский сокет
            if (err == 0)
                pvt->flags |= DMM_NETIPSEND_HASSOCK;
            */
			resp = DMM_MSG_CREATE_RESP(DMM_NODE_ID(node), msg, 0); //хотим узнать, нужно ли нам отправлять ответ resp
            if (resp != NULL) { //если Диммон хочет, чтобы мы ответили, то ответим
                if (err != 0)
                    msg->cm_flags |= DMM_MSG_ERR; //если не удалось создать сокет, ставим флаг

                DMM_MSG_SEND_ID(msg->cm_src, resp); //отправляем ответ resp, если от нас того хотели
            } else
                err = (err != 0) ? err : ENOMEM;
            break;

        case DMM_MSG_NETIPSEND_CONNECT: {
            struct dmm_msg_netipsend_connect *nc;
            nc = DMM_MSG_DATA(msg, struct dmm_msg_netipsend_connect); // достаем указатель на место, где лежит информация об адресе, куда будем коннектиться
            err = connect_socket(node, pvt->fd, nc->addr); //конектимся к тому адресу, куда нам сказал Диммон
            if (err == 0)
			{
				pvt->flags |= DMM_NETIPSEND_HASSOCK;
                pvt->flags |= DMM_NETIPSEND_CONNECTED; //если все прошло без ошибок, то ставим соответствующий флаг
            }
			resp = DMM_MSG_CREATE_RESP(DMM_NODE_ID(node), msg, 0); //аналогично отправляем resp, если от нас того хотят 
            if (resp != NULL) 
			{
                if (err != 0)
                    msg->cm_flags |= DMM_MSG_ERR;

                DMM_MSG_SEND_ID(msg->cm_src, resp);
            } else
                err = (err != 0) ? err : ENOMEM;
            break;
        }

        default:
            err = ENOTSUP;
            break;
        }
        break;

    default:
        err = ENOTSUP;
        break;
    }

    DMM_MSG_FREE(msg);
    return err;
}

struct dmm_type send_type = {
    "net/ip/send",
    send_ctor,
    send_dtor,
    send_rcvdata,
    send_rcvmsg,
    send_newhook,
    NULL,
    {},
};
