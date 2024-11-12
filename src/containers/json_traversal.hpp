#pragma once

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>

using json = nlohmann::json;
#define RDMA_ADAPTER "ens1f1np1"

class ConfigParser
{
public:
    struct Host
    {
        std::string host;
        int memory_port;
        int metadata_port;
    };

    // Vector to hold hosts
    std::vector<Host> hosts;
    std::string my_ip;

    // Constructor that loads JSON configuration from a file and initializes hosts
    explicit ConfigParser(const std::string &filename)
    {
        my_ip = get_ip_address(RDMA_ADAPTER);
        json j = load_from_file(filename);
        if (!j.is_null())
        {
            initialize_hosts(j);
        }
    }

    std::vector<Host> get_hosts()
    {
        return hosts;
    }

    void print_hosts()
    {
        for (const auto &host : hosts)
        {
            std::cout << "Host: " << host.host << ", Memory Port: " << host.memory_port << ", Metadata Port: " << host.metadata_port << std::endl;
        }
    }

    // Load JSON from a file
    static json load_from_file(const std::string &filename)
    {
        std::ifstream file(filename);
        if (!file.is_open())
        {
            std::cerr << "Error opening file: " << filename << " make sure that the config file is in /mydata/config.json" << std::endl;

            return nullptr;
        }
        json j;
        file >> j;
        return j;
    }

    // Load JSON from a string
    static json load_from_string(const std::string &data)
    {
        return json::parse(data, nullptr, false);
    }

    // Initialize the hosts vector from JSON
    void initialize_hosts(const json &j)
    {
        if (j.contains("hosts") && j["hosts"].is_array())
        {
            for (const auto &host_data : j["hosts"])
            {
                if (host_data.contains("host") && host_data.contains("Memory_port") && host_data.contains("metadata_port"))
                {
                    std::string host_ip = host_data["host"].get<std::string>();
                    if (host_ip != my_ip)
                    {
                        Host host{
                            host_ip,
                            host_data["Memory_port"].get<int>(),
                            host_data["metadata_port"].get<int>()};
                        hosts.push_back(host);
                    }
                }
                else
                {
                    std::cerr << "Invalid host entry in configuration file." << std::endl;
                }
            }
        }
        else
        {
            std::cerr << "No hosts found in configuration file or 'hosts' is not an array." << std::endl;
        }
    }

    // Find a value by key, including in nested objects
    static json find_key(const json &j, const std::string &key)
    {
        if (j.is_object())
        {
            if (j.contains(key))
            {
                return j.at(key);
            }
            // Recursively search in nested objects
            for (const auto &item : j.items())
            {
                if (item.value().is_object() || item.value().is_array())
                {
                    json found = find_key(item.value(), key);
                    if (!found.is_null())
                    {
                        return found;
                    }
                }
            }
        }
        else if (j.is_array())
        {
            // Recursively search in each array element
            for (const auto &element : j)
            {
                json found = find_key(element, key);
                if (!found.is_null())
                {
                    return found;
                }
            }
        }
        return nullptr; // Return null if key is not found
    }

    // Traverse all key-value pairs and apply a callback function
    static void traverse(const json &j, const std::function<void(const std::string &, const json &)> &callback)
    {
        if (j.is_object())
        {
            for (const auto &item : j.items())
            {
                callback(item.key(), item.value());
                traverse(item.value(), callback); // Recurse for nested objects/arrays
            }
        }
        else if (j.is_array())
        {
            for (const auto &element : j)
            {
                traverse(element, callback); // Recurse for each array element
            }
        }
    }

    // Print all keys and values in JSON object (for testing)
    static void print(const json &j)
    {
        traverse(j, [](const std::string &key, const json &value)
                 { std::cout << "Key: " << key << ", Value: " << value << std::endl; });
    }

    std::string get_ip_address(const std::string &interface_name)
    {
        struct ifaddrs *ifAddrStruct = nullptr;
        struct ifaddrs *ifa = nullptr;
        void *tmpAddrPtr = nullptr;
        std::string ip_address;

        getifaddrs(&ifAddrStruct);

        for (ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next)
        {
            if (!ifa->ifa_addr)
            {
                continue;
            }
            if (ifa->ifa_addr->sa_family == AF_INET && strcmp(ifa->ifa_name, interface_name.c_str()) == 0)
            {
                tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
                char addressBuffer[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
                ip_address = addressBuffer;
                break;
            }
        }

        if (ifAddrStruct != nullptr)
        {
            freeifaddrs(ifAddrStruct);
        }

        return ip_address;
    }
};
