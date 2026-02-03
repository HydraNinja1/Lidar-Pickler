#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <filesystem>
#include <nlohmann/json.hpp>

namespace fs = std::filesystem;
using json = nlohmann::json;

class LidarReplay : public rclcpp::Node {
public:
    LidarReplay() : Node("lidar_replay_node") {
        parent_dir_ = this->declare_parameter<std::string>("parent_dir", ".");
        lidar_dir_ = parent_dir_+ "/lidar_frames_out";
        imu_dir_ = parent_dir_ + "/imu_frames_out";
        frame_id_override_ = this->declare_parameter<std::string>("frame_id_override", "");

        load_lidar_metadata();

        lidar_pub_ = this->create_publisher<sensor_msgs::msg::PointCloud2>("/ouster/points", rclcpp::SensorDataQoS());
        imu_pub_ = this->create_publisher<sensor_msgs::msg::Imu>("/ouster/imu", rclcpp::SensorDataQoS());

        for (const auto& entry : fs::directory_iterator(lidar_dir_)) {
            if (entry.path().extension() == ".bin") {
                auto [ts, valid] = extract_timestamp(entry.path().filename().string());
                if (valid) replay_queue_.push_back({ts, TimedMessage::Type::LIDAR, entry.path()});
            }
        }

        for (const auto& entry : fs::directory_iterator(imu_dir_)) {
            if (entry.path().extension() == ".json") {
                auto [ts, valid] = extract_timestamp(entry.path().filename().string());
                if (valid) replay_queue_.push_back({ts, TimedMessage::Type::IMU, entry.path()});
            }
        }

        std::sort(replay_queue_.begin(), replay_queue_.end(),
                  [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });

        if (replay_queue_.empty()) {
            RCLCPP_ERROR(this->get_logger(), "No data to replay.");
            return;
        }

        base_time_ = replay_queue_.front().timestamp;
        replay_start_wall_time_ = std::chrono::steady_clock::now();

        replay_timer_ = this->create_wall_timer(
            std::chrono::milliseconds(1),
            std::bind(&LidarReplay::scheduled_publish, this));

        RCLCPP_INFO(this->get_logger(), "Loaded %zu messages for replay.", replay_queue_.size());
    }

private:
    struct TimedMessage {
        rclcpp::Time timestamp;
        enum class Type { LIDAR, IMU } type;
        fs::path filepath;
    };

    void load_lidar_metadata() {
        std::ifstream file(lidar_dir_ + "/metadata.json");
        json j;
        file >> j;

        meta_.height = j["height"];
        meta_.width = j["width"];
        meta_.is_bigendian = j["is_bigendian"];
        meta_.is_dense = j["is_dense"];
        meta_.point_step = j["point_step"];
        meta_.row_step = j["row_step"];
        meta_.header.frame_id = j["frame_id"];

        for (const auto& f : j["fields"]) {
            sensor_msgs::msg::PointField field;
            field.name = f["name"];
            field.offset = f["offset"];
            field.datatype = f["datatype"];
            field.count = f["count"];
            meta_.fields.push_back(field);
        }
    }

    void scheduled_publish() {
        auto now = std::chrono::steady_clock::now();
        rclcpp::Duration elapsed = rclcpp::Duration::from_nanoseconds(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now - replay_start_wall_time_).count());

        while (next_index_ < replay_queue_.size() &&
               replay_queue_[next_index_].timestamp <= base_time_ + elapsed) {

            const auto& msg = replay_queue_[next_index_++];
            if (msg.type == TimedMessage::Type::LIDAR)
                publish_lidar_from_file(msg.filepath, msg.timestamp);
            else
                publish_imu_from_file(msg.filepath, msg.timestamp);
        }

        if (next_index_ >= replay_queue_.size()) {
            RCLCPP_INFO(this->get_logger(), "Finished scheduled replay.");
            rclcpp::shutdown();
        }
    }

    void publish_lidar_from_file(const fs::path& filepath, const rclcpp::Time& timestamp) {
        std::ifstream in(filepath, std::ios::binary);
        if (!in) return;

        std::vector<uint8_t> buffer(meta_.height * meta_.width * meta_.point_step);
        in.read(reinterpret_cast<char*>(buffer.data()), buffer.size());
        in.close();

        sensor_msgs::msg::PointCloud2 msg;
        msg.header.stamp = timestamp;
        msg.header.frame_id = frame_id_override_.empty() ? meta_.header.frame_id : frame_id_override_;
        msg.height = meta_.height;
        msg.width = meta_.width;
        msg.is_bigendian = meta_.is_bigendian;
        msg.is_dense = meta_.is_dense;
        msg.point_step = meta_.point_step;
        msg.row_step = meta_.row_step;
        msg.fields = meta_.fields;
        msg.data = std::move(buffer);

        lidar_pub_->publish(msg);
    }

    void publish_imu_from_file(const fs::path& filepath, const rclcpp::Time& timestamp) {
        std::ifstream in(filepath);
        if (!in) {
            RCLCPP_WARN(this->get_logger(), "Failed to open IMU file: %s", filepath.c_str());
            return;
        }

        json j;
        try {
            in >> j;
        } catch (const json::parse_error& e) {
            RCLCPP_WARN(this->get_logger(), "Failed to parse IMU JSON file: %s. Error: %s", filepath.c_str(), e.what());
            return;  // Skip publishing this file
        }

        try {
            sensor_msgs::msg::Imu msg;
            msg.header.stamp = timestamp;
            msg.header.frame_id = j["frame_id"];

            msg.orientation.x = j["orientation"]["x"];
            msg.orientation.y = j["orientation"]["y"];
            msg.orientation.z = j["orientation"]["z"];
            msg.orientation.w = j["orientation"]["w"];
            msg.orientation_covariance = j["orientation_covariance"];

            msg.angular_velocity.x = j["angular_velocity"]["x"];
            msg.angular_velocity.y = j["angular_velocity"]["y"];
            msg.angular_velocity.z = j["angular_velocity"]["z"];
            msg.angular_velocity_covariance = j["angular_velocity_covariance"];

            msg.linear_acceleration.x = j["linear_acceleration"]["x"];
            msg.linear_acceleration.y = j["linear_acceleration"]["y"];
            msg.linear_acceleration.z = j["linear_acceleration"]["z"];
            msg.linear_acceleration_covariance = j["linear_acceleration_covariance"];

            imu_pub_->publish(msg);
        } catch (const json::exception& e) {
            RCLCPP_WARN(this->get_logger(), "Incomplete or malformed data in IMU JSON file: %s. Error: %s", filepath.c_str(), e.what());
            return;  // Skip publishing malformed data
        }
    }

    std::pair<rclcpp::Time, bool> extract_timestamp(const std::string& filename) {
        try {
            size_t first = filename.find('_');
            size_t second = filename.find('_', first + 1);
            size_t dot = filename.find('.', second);
            if (first == std::string::npos || second == std::string::npos || dot == std::string::npos) return {{0,0}, false};
            uint32_t sec = std::stoul(filename.substr(first + 1, second - first - 1));
            uint32_t nsec = std::stoul(filename.substr(second + 1, dot - second - 1));
            return {rclcpp::Time(sec, nsec), true};
        } catch (...) {
            return {{0, 0}, false};
        }
    }

    rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr lidar_pub_;
    rclcpp::Publisher<sensor_msgs::msg::Imu>::SharedPtr imu_pub_;
    rclcpp::TimerBase::SharedPtr replay_timer_;

    std::string lidar_dir_;
    std::string imu_dir_;
    std::string frame_id_override_;

    std::vector<TimedMessage> replay_queue_;
    size_t next_index_ = 0;

    rclcpp::Time base_time_;
    std::chrono::steady_clock::time_point replay_start_wall_time_;

    sensor_msgs::msg::PointCloud2 meta_;
};

int main(int argc, char* argv[]) {
    rclcpp::init(argc, argv);
    auto node = std::make_shared<LidarReplay>();
    
    // using Multi-threaded Executor allows multiple callbacks to run in parallel using separate threads
    rclcpp::executors::MultiThreadedExecutor executor;
    executor.add_node(node);
    executor.spin();
    rclcpp::shutdown();
    return 0;
}
