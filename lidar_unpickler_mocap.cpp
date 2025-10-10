#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>

#include "rclcpp/rclcpp.hpp"
#include "geometry_msgs/msg/pose_stamped.hpp"

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using geometry_msgs::msg::PoseStamped;

struct MocapPose {
    uint32_t sec;
    uint32_t nsec;
    double px, py, pz;
    double ox, oy, oz, ow;
};

class MocapReplay : public rclcpp::Node {
public:
    MocapReplay() : Node("mocap_replay_node"), index_(0)
    {
        mocap_dir_ = this->declare_parameter<std::string>("mocap_dir", "./mocap_frames_out");

        pub_ = this->create_publisher<PoseStamped>("/vrpn_mocap/tallBoy/pose", rclcpp::SensorDataQoS());

        if (!load_csv_directory(mocap_dir_)) {
            RCLCPP_ERROR(this->get_logger(), "Failed to load any CSV files in directory: %s", mocap_dir_.c_str());
            rclcpp::shutdown();
            return;
        }

        RCLCPP_INFO(this->get_logger(), "Loaded %zu mocap poses from %zu files.", poses_.size(), csv_files_.size());

        // 56 Hz -> 1000 / 56 ≈ 17.857 ms
        timer_ = this->create_wall_timer(
            std::chrono::milliseconds(17),
            std::bind(&MocapReplay::publish_next_pose, this)
        );
    }

private:
    bool load_csv_directory(const std::string &dir) {
        if (!fs::exists(dir) || !fs::is_directory(dir)) return false;

        // Collect all CSV files
        for (const auto &entry : fs::directory_iterator(dir)) {
            if (entry.path().extension() == ".csv") {
                csv_files_.push_back(entry.path());
            }
        }

        if (csv_files_.empty()) return false;

        // Sort alphabetically (assumes sequential filenames)
        std::sort(csv_files_.begin(), csv_files_.end());

        // Load all CSVs
        for (const auto &csv : csv_files_) {
            if (!load_csv(csv.string())) {
                RCLCPP_WARN(this->get_logger(), "Failed to load CSV: %s", csv.string().c_str());
            }
        }

        return !poses_.empty();
    }

    bool load_csv(const std::string &filename) {
        std::ifstream file(filename);
        if (!file.is_open()) return false;

        std::string line;
        std::getline(file, line);  // skip header

        while (std::getline(file, line)) {
            std::stringstream ss(line);
            std::string item;
            MocapPose pose;

            std::getline(ss, item, ','); pose.sec = std::stoul(item);
            std::getline(ss, item, ','); pose.nsec = std::stoul(item);
            std::getline(ss, item, ','); pose.px = std::stod(item);
            std::getline(ss, item, ','); pose.py = std::stod(item);
            std::getline(ss, item, ','); pose.pz = std::stod(item);
            std::getline(ss, item, ','); pose.ox = std::stod(item);
            std::getline(ss, item, ','); pose.oy = std::stod(item);
            std::getline(ss, item, ','); pose.oz = std::stod(item);
            std::getline(ss, item, ','); pose.ow = std::stod(item);

            poses_.push_back(pose);
        }
        return !poses_.empty();
    }

    void publish_next_pose() {
        if (index_ >= poses_.size()) {
            RCLCPP_INFO(this->get_logger(), "Finished replaying mocap data.");
            rclcpp::shutdown();
            return;
        }

        const auto &p = poses_[index_++];
        PoseStamped msg;

        msg.header.stamp.sec = p.sec;
        msg.header.stamp.nanosec = p.nsec;
        msg.header.frame_id = "vrpn_mocap_out";

        msg.pose.position.x = p.px;
        msg.pose.position.y = p.py;
        msg.pose.position.z = p.pz;

        msg.pose.orientation.w = p.ow;
        msg.pose.orientation.x = p.ox;
        msg.pose.orientation.y = p.oy;
        msg.pose.orientation.z = p.oz;

        pub_->publish(msg);
    }

    rclcpp::Publisher<PoseStamped>::SharedPtr pub_;
    rclcpp::TimerBase::SharedPtr timer_;
    std::vector<MocapPose> poses_;
    std::vector<fs::path> csv_files_;
    size_t index_;
    std::string mocap_dir_;
};

int main(int argc, char *argv[]) {
    rclcpp::init(argc, argv);
    auto node = std::make_shared<MocapReplay>();
    rclcpp::spin(node);
    rclcpp::shutdown();
    return 0;
}
