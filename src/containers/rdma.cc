// rdma.cpp
#include "rdma.hpp"
#include <infiniband/verbs.h> // For IBV functions

// Constructor
RDMAConnection::RDMAConnection(const std::string &ip, uint64_t index, bool isLocal)
    : ip(ip), index(index), context(nullptr), qp(nullptr), qp_factory(nullptr),
      remote_buffer_token(nullptr), buffer(nullptr), isLocal(isLocal)
{
    // Initialization can be deferred to the init function
}

// Destructor
RDMAConnection::~RDMAConnection()
{
    // Clean up RDMA resources
    delete buffer;
    delete qp;
    delete qp_factory;
    delete context;
}

// Static function to find NIC containing a specific string
std::string RDMAConnection::findNICContaining(const std::string &s)
{
    int32_t numberOfInstalledDevices = 0;
    ibv_device **ibvDeviceList = ibv_get_device_list(&numberOfInstalledDevices);

    for (int dev_i = 0; dev_i < numberOfInstalledDevices; dev_i++)
    {
        ibv_device *dev = ibvDeviceList[dev_i];
        const char *name = ibv_get_device_name(dev);
        if (std::string(name).find(s) != std::string::npos)
        {
            ibv_free_device_list(ibvDeviceList);
            return std::string(name);
        }
    }
    ibv_free_device_list(ibvDeviceList);
    return "";
}

void RDMAConnection::init(void *memory_region, uint64_t size, int server_port)
{
    std::lock_guard<std::mutex> lock(rdma_mutex); // Ensure thread safety

    std::string device_name_set = "mlx5_3";
    std::string device_name = findNICContaining(device_name_set);
    if (device_name.empty())
    {
        std::cerr << "No device found with name: " << device_name_set << std::endl;
        exit(1);
    }

    std::cout << "Using device: " << device_name << std::endl;

    // Create RDMA context and queue pair factory
    context = new infinity::core::Context(device_name);
    qp_factory = new infinity::queues::QueuePairFactory(context);
    std::cout << "Created RDMA context and queue pair factory." << std::endl;

    // Create RDMA buffer and region token
    buffer = new infinity::memory::Buffer(context, memory_region, size);

    remote_buffer_token = buffer->createRegionToken();
    std::cout << "Created RDMA buffer and region token." << std::endl;

    // Bind to port and accept incoming connection
    qp_factory->bindToPort(server_port);
    std::cout << "Bound to port: " << server_port << std::endl;
    std::cout << "Waiting for incoming connection..." << std::endl;

    qp = qp_factory->acceptIncomingConnection(remote_buffer_token, sizeof(infinity::memory::RegionToken));
    std::cout << "Accepted incoming connection." << std::endl;
}
