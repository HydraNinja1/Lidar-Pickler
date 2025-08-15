#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include <fstream>
#include <sstream>
#include <string>
#include <chrono>
#include <filesystem>  // C++17
#include <nlohmann/json.hpp>

namespace fs = std::filesystem;
using json = nlohmann::json;

class LidarPickler : public rclcpp::Node {
public:
    LidarPickler() : Node("lidar_pickler_node"){
        
        // set up output directories - the path is passed as an argument 
        lidar_dir_ = this->declare_parameter<std::string>("lidar_dir", "./lidar_frames_out");
        imu_dir_ = this->declare_parameter<std::string>("imu_dir", "./imu_frames_out");

        prepare_output_dir(lidar_dir_);
        prepare_output_dir(imu_dir_);
        
        // est up the qos profle this will be dependant on how the publisher of the topic is set up.
        rclcpp::SensorDataQoS qos;
        
        // set up the point cloud subscriber
        lidar_sub_ = this->create_subscription<sensor_msgs::msg::PointCloud2>(
            "/ouster/points", rclcpp::SensorDataQoS(),
            std::bind(&LidarPickler::lidar_callback, this, std::placeholders::_1)
        );
        
        // set up the imu subscriber
        imu_sub_ = this->create_subscription<sensor_msgs::msg::Imu>(
            "/ouster/imu", rclcpp::SensorDataQoS(),
            std::bind(&LidarPickler::imu_callback, this, std::placeholders::_1)
        );

        RCLCPP_INFO(this->get_logger(), "Lidar + IMU Pickler has been initialised.");
    }

private:
    void lidar_callback(const sensor_msgs::msg::PointCloud2::SharedPtr msg) {
        save_metadata_once(msg);
        
        auto timestamp = msg->header.stamp;

        char filename_char[128];
        snprintf(filename_char, sizeof(filename_char), "%s/scan_%010u_%09u.bin",
                lidar_dir_.c_str(), timestamp.sec, timestamp.nanosec);

        std::ofstream out(filename_char, std::ios::out | std::ios::binary | std::ios::trunc);
        if (out.is_open()) {
            out.write(reinterpret_cast<const char*>(msg->data.data()), msg->data.size());
            out.close();
        } else {
            RCLCPP_ERROR(this->get_logger(), "Failed to open file: %s", filename_char);
        }
    }

    void imu_callback(const sensor_msgs::msg::Imu::SharedPtr msg) {
        auto timestamp = msg->header.stamp;

        char filename_char[128];
        snprintf(filename_char, sizeof(filename_char), "%s/imu_%010u_%09u.json",
                imu_dir_.c_str(), timestamp.sec, timestamp.nanosec);

        json j;
        j["frame_id"] = msg->header.frame_id;
        j["timestamp_sec"] = msg->header.stamp.sec;
        j["timestamp_nanosec"] = msg->header.stamp.nanosec;

        j["orientation"] = {
            {"x", msg->orientation.x}, {"y", msg->orientation.y},
            {"z", msg->orientation.z}, {"w", msg->orientation.w}
        };
        j["orientation_covariance"] = msg->orientation_covariance;
        j["angular_velocity"] = {
            {"x", msg->angular_velocity.x}, {"y", msg->angular_velocity.y}, {"z", msg->angular_velocity.z}
        };
        j["angular_velocity_covariance"] = msg->angular_velocity_covariance;
        j["linear_acceleration"] = {
            {"x", msg->linear_acceleration.x}, {"y", msg->linear_acceleration.y}, {"z", msg->linear_acceleration.z}
        };
        j["linear_acceleration_covariance"] = msg->linear_acceleration_covariance;

        std::ofstream file(filename_char);
        file << j.dump(4);
        file.flush();
        file.close();
    }

    void prepare_output_dir(const std::string& dir) {
        if (!fs::exists(dir)) {
            if (!fs::create_directories(dir)) {
                RCLCPP_ERROR(this->get_logger(), "Failed to create output directory: %s", dir.c_str());
            } else {
                RCLCPP_INFO(this->get_logger(), "Created output directory: %s", dir.c_str());
            }
        }
    }

    void save_metadata_once(const sensor_msgs::msg::PointCloud2::SharedPtr msg) {
        static bool saved = false;
        if (saved) return;

        json j;
        j["height"] = msg->height;
        j["width"] = msg->width;
        j["is_bigendian"] = msg->is_bigendian;
        j["point_step"] = msg->point_step;
        j["row_step"] = msg->row_step;
        j["is_dense"] = msg->is_dense;
        j["frame_id"] = msg->header.frame_id;

        for (const auto& field : msg->fields) {
            j["fields"].push_back({
                {"name", field.name},
                {"offset", field.offset},
                {"datatype", field.datatype},
                {"count", field.count}
            });
        }

        std::ofstream file(lidar_dir_ + "/metadata.json");
        file << j.dump(4);  // Pretty-print with 4-space indentation
        file.close();
        saved = true;
    }

    rclcpp::Subscription<sensor_msgs::msg::PointCloud2>::SharedPtr lidar_sub_;
    rclcpp::Subscription<sensor_msgs::msg::Imu>::SharedPtr imu_sub_;
    // int file_ind_;
    std::string lidar_dir_;
    std::string imu_dir_;

};
    
int main(int argc, char* argv[]) {
    rclcpp::init(argc, argv);
    rclcpp::spin(std::make_shared<LidarPickler>());
    rclcpp::shutdown();
    return 0;
}
