// rdma.cpp
#include "rdma.hpp"
#include <infiniband/verbs.h> // For IBV functions
#include <fstream>            // For std::ofstream

// Constructor
RDMAServer::RDMAServer(const std::string &ip, uint64_t index, bool isLocal)
    : ip(ip), index(index), context(nullptr), qp(nullptr), qp_factory(nullptr),
      remote_buffer_token(nullptr), buffer(nullptr), isLocal(isLocal)
{
    // Initialization can be deferred to the init function
}

// Destructor
RDMAServer::~RDMAServer()
{
    // Clean up RDMA resources
    delete buffer;
    delete qp;
    delete qp_factory;
    delete context;
}

// Static function to find NIC containing a specific string
std::string RDMAServer::findNICContaining(const std::string &s)
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

void RDMAServer::init(void *memory_region, uint64_t size, int server_port, int expected_connections)
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

    // qp = qp_factory->acceptIncomingConnection(remote_buffer_token, sizeof(infinity::memory::RegionToken));
    // std::cout << "Accepted incoming connection." << std::endl;
    for (int i = 0; i < expected_connections; ++i)
    {

        std::cout << "Waiting for incoming connection..." << std::endl;
        infinity::queues::QueuePair *new_qp = qp_factory->acceptIncomingConnection(remote_buffer_token, sizeof(infinity::memory::RegionToken));
        std::cout << "Accepted incoming connection " << (i + 1) << std::endl;
        qp_list.push_back(new_qp);
    }
}

// -------------------------------------------------- RDMAClient --------------------------------------------------
RDMAClient::RDMAClient(const std::string &ip, uint16_t port, bool isMetaData)
    : ip(ip), port(port), device_hint("mlx5_3"), output_file("dump.txt"),
      context(nullptr), qp(nullptr), qp_factory(nullptr), remote_buffer_token(nullptr),
      meta_data_buffer(nullptr), page_buffer(nullptr), request_token(nullptr) {}

// RDMAClient Destructor
RDMAClient::~RDMAClient()
{
    if (page_buffer != nullptr)
    {
        delete page_buffer;
    }
    if (meta_data_buffer != nullptr)
    {
        delete meta_data_buffer;
    }
    if (request_token != nullptr)
    {
        delete request_token;
    }
    if (qp != nullptr)
    {
        delete qp;
    }
    if (qp_factory != nullptr)
    {
        delete qp_factory;
    }
    if (context != nullptr)
    {
        delete context;
    }
}

bool RDMAClient::connectToServer()
{
    // Find the NIC containing the specified device hint
    int32_t numberOfInstalledDevices = 0;
    ibv_device **ibvDeviceList = ibv_get_device_list(&numberOfInstalledDevices);

    std::string device_name;
    for (int dev_i = 0; dev_i < numberOfInstalledDevices; dev_i++)
    {
        ibv_device *dev = ibvDeviceList[dev_i];
        const char *name = ibv_get_device_name(dev);
        if (std::string(name).find(device_hint) != std::string::npos)
        {
            device_name = std::string(name);
            break;
        }
    }
    ibv_free_device_list(ibvDeviceList);

    if (device_name.empty())
    {
        std::cerr << "No device found containing: " << device_hint << std::endl;
        return false;
    }

    std::cout << "Using device: " << device_name << std::endl;

    // Initialize RDMA context and queue pair factory
    context = new infinity::core::Context(device_name);
    qp_factory = new infinity::queues::QueuePairFactory(context);

    // Connect to the remote server
    std::cout << "Connecting to server at " << ip << ":" << port << std::endl;
    qp = qp_factory->connectToRemoteHost(ip.c_str(), port);
    std::cout << "Connected to server." << std::endl;

    // Retrieve the remote memory region token from the server
    remote_buffer_token = static_cast<infinity::memory::RegionToken *>(qp->getUserData());
    if (remote_buffer_token == nullptr)
    {
        std::cerr << "Failed to get remote buffer token from server." << std::endl;
        return false;
    }
    std::cout << "Received remote buffer token from server." << std::endl;

    // Allocate a fixed 64KB local buffer for RDMA reads
    const uint64_t buffer_size = CLIENT_BUFFER_SIZE; // 64KB
    void *local_buffer_memory = malloc(buffer_size);
    if (local_buffer_memory == nullptr)
    {
        std::cerr << "Failed to allocate local buffer memory." << std::endl;
        return false;
    }
    std::cout << "Allocated local buffer memory." << std::endl;

    void *meta_data_buffer_memory = malloc(MAX_BLOCKS * sizeof(size_t));

    std::cout << "Allocated metadata buffer memory." << std::endl;

    page_buffer = new infinity::memory::Buffer(context, local_buffer_memory, buffer_size);
    std::cout << "Created page buffer." << std::endl;
    meta_data_buffer = new infinity::memory::Buffer(context, meta_data_buffer_memory, MAX_BLOCKS * sizeof(size_t));
    std::cout << "Created metadata buffer." << std::endl;
    request_token = new infinity::requests::RequestToken(context);
    std::cout << "Created request token." << std::endl;

    std::cout << "Client initialization complete." << std::endl;

    return true;
}

void RDMAClient::performRDMARead(uint64_t total_buffer_size)
{
    std::ofstream output(output_file, std::ios::out | std::ios::binary);
    if (!output)
    {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        return;
    }

    uint64_t offset = 0;
    uint64_t buffer_size = page_buffer->getSizeInBytes();

    while (offset < total_buffer_size)
    {
        uint64_t current_chunk_size = std::min(buffer_size, total_buffer_size - offset);

        std::cout << "Performing RDMA read (offset " << offset << ", size " << current_chunk_size << ")..." << std::endl;
        qp->read(page_buffer, 0, remote_buffer_token, offset, current_chunk_size, infinity::queues::OperationFlags(), request_token);
        request_token->waitUntilCompleted();

        output.write(static_cast<char *>(page_buffer->getData()), current_chunk_size);
        offset += current_chunk_size;
    }

    output.close();
    std::cout << "Data written to file: " << output_file << std::endl;
}

void RDMAClient::readMetadata()
{
    std::ofstream output("metadata.txt", std::ios::out | std::ios::binary);
    if (!output)
    {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        return;
    }

    uint64_t offset = 0;
    uint64_t buffer_size = meta_data_buffer->getSizeInBytes();

    std::cout << "Performing RDMA read (offset " << offset << ", size " << buffer_size << ")..." << std::endl;
    qp->read(meta_data_buffer, 0, remote_buffer_token, offset, buffer_size, infinity::queues::OperationFlags(), request_token);
    request_token->waitUntilCompleted();

    output.write(static_cast<char *>(meta_data_buffer->getData()), buffer_size);

    output.close();
    std::cout << "Data written to file: " << output_file << std::endl;
}