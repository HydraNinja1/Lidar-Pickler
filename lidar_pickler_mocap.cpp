#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include <geometry_msgs/msg/pose_stamped.hpp>
#include <fstream>
#include <sstream>
#include <string>
#include <chrono>
#include <filesystem>  // C++17
#include <nlohmann/json.hpp>
#include <iomanip>

namespace fs = std::filesystem;
using json = nlohmann::json;
using std::placeholders::_1;

class LidarPickler : public rclcpp::Node {
public:
    LidarPickler() : Node("lidar_pickler_node"){
        
        // set up output directories paths as arguments
        lidar_dir_ = this->declare_parameter<std::string>("lidar_dir", "./lidar_frames_out");
        imu_dir_ = this->declare_parameter<std::string>("imu_dir", "./imu_frames_out");
        mocap_dir_  = this->declare_parameter<std::string>("mocap_dir",  "./mocap_frames_out");

        // set up topic parameters as arguments
        lidar_topic_ = this->declare_parameter<std::string>("lidar_topic", "/ouster/points");
        imu_topic_ = this->declare_parameter<std::string>("imu_topic", "/ouster/imu");
        mocap_topic_ = this->declare_parameter<std::string>("pose_topic", "/vrpn_mocap/tallBoy/pose");

        // setting up the output directories
        prepare_output_dir(lidar_dir_);
        prepare_output_dir(imu_dir_);
        prepare_output_dir(mocap_dir_);

        // prepare mocap CSV (write header once if missing)
        mocap_csv_path_ = (fs::path(mocap_dir_) / "mocap_poses.csv").string();
        if (!fs::exists(mocap_csv_path_)) {
            std::ofstream csv(mocap_csv_path_, std::ios::out | std::ios::trunc);
            if (!csv.is_open()) {
                RCLCPP_ERROR(this->get_logger(), "Failed to create CSV: %s", mocap_csv_path_.c_str());
            } else {
                csv << "timestamp_sec,timestamp_nanosec,pos_x,pos_y,pos_z,ori_x,ori_y,ori_z,ori_w\n";
                csv.close();
            }
        }
        
        // sst up the qos profle this will be dependant on how the publisher of the topic is set up.
        rclcpp::SensorDataQoS qos;
        
        // set up the point cloud subscriber
        lidar_sub_ = this->create_subscription<sensor_msgs::msg::PointCloud2>(
            lidar_topic_, qos, std::bind(&LidarPickler::lidar_callback, this, _1)
        );
        
        // set up the imu subscriber
        imu_sub_ = this->create_subscription<sensor_msgs::msg::Imu>(
            imu_topic_, qos, std::bind(&LidarPickler::imu_callback, this, _1)
        );

        // set up the mocap subscriber 
        mocap_sub_ = this->create_subscription<geometry_msgs::msg::PoseStamped>(
            mocap_topic_, qos, std::bind(&LidarPickler::mocap_callback, this, _1)
        );

        RCLCPP_INFO(this->get_logger(), "Lidar, IMU + MOCAP Pickler has been initialised.");
    }

private:
    void lidar_callback(const sensor_msgs::msg::PointCloud2::SharedPtr msg) {
        save_metadata_once(msg);
        
        auto timestamp = msg->header.stamp;

        char filename_char[128];
        snprintf(filename_char, sizeof(filename_char), "%s/scan_%010d_%09u.bin",
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
        snprintf(filename_char, sizeof(filename_char), "%s/imu_%010d_%09u.json",
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
        if (!file.is_open()) {
            RCLCPP_ERROR(this->get_logger(), "Failed to open file: %s", filename_char);
            return;
        }
        file << j.dump(4);
        file.close();
    }

    void mocap_callback(const geometry_msgs::msg::PoseStamped::SharedPtr msg) {
        const auto& t = msg->header.stamp;

        std::ofstream csv(mocap_csv_path_, std::ios::out | std::ios::app);
        if (!csv.is_open()) {
            RCLCPP_ERROR(this->get_logger(), "Failed to open CSV for appending: %s", mocap_csv_path_.c_str());
            return;
        }

        csv << std::fixed << std::setprecision(9)
            << t.sec << ","
            << t.nanosec << ","
            << msg->pose.position.x << ","
            << msg->pose.position.y << ","
            << msg->pose.position.z << ","
            << msg->pose.orientation.x << ","
            << msg->pose.orientation.y << ","
            << msg->pose.orientation.z << ","
            << msg->pose.orientation.w << "\n";
        csv.close();
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
        if (!file.is_open()) {
            RCLCPP_ERROR(this->get_logger(), "Failed to open metadata file for writing");
            return;
        }
        file << j.dump(4);  // Pretty-print with 4-space indentation
        file.close();
        saved = true;
    }

    rclcpp::Subscription<sensor_msgs::msg::PointCloud2>::SharedPtr lidar_sub_;
    rclcpp::Subscription<sensor_msgs::msg::Imu>::SharedPtr imu_sub_;
    rclcpp::Subscription<geometry_msgs::msg::PoseStamped>::SharedPtr mocap_sub_;

    std::string lidar_dir_;
    std::string imu_dir_;
    std::string mocap_dir_;
    std::string lidar_topic_;
    std::string imu_topic_;
    std::string mocap_topic_;
    std::string mocap_csv_path_;

};
    
int main(int argc, char* argv[]) {
    rclcpp::init(argc, argv);
    rclcpp::spin(std::make_shared<LidarPickler>());
    rclcpp::shutdown();
    return 0;
}
