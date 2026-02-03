#include <rclcpp/rclcpp.hpp>

#include <sensor_msgs/msg/point_cloud2.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include <geometry_msgs/msg/pose_stamped.hpp>

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <algorithm>
#include <chrono>

namespace fs = std::filesystem;
using json = nlohmann::json;

class LidarPicklerRepublisher : public rclcpp::Node
{
public:
    LidarPicklerRepublisher()
    : Node("lidar_pickler_republisher")
    {
        lidar_dir_ = declare_parameter<std::string>("lidar_dir", "./lidar_frames_out");
        imu_dir_   = declare_parameter<std::string>("imu_dir", "./imu_frames_out");
        csv_dir_   = declare_parameter<std::string>("csvs_dir", "./csvs_out");
        loop_      = declare_parameter<bool>("loop", true);

        lidar_pub_ = create_publisher<sensor_msgs::msg::PointCloud2>(
            "/ouster/points", rclcpp::SensorDataQoS());

        imu_pub_ = create_publisher<sensor_msgs::msg::Imu>(
            "/ouster/imu", rclcpp::SensorDataQoS());

        mocap_pub_ = create_publisher<geometry_msgs::msg::PoseStamped>(
            "/vrpn_mocap/Octopus/pose", rclcpp::SensorDataQoS());

        load_lidar();
        load_imu();
        load_mocap();

        RCLCPP_INFO(get_logger(),
            "Loaded %zu lidar, %zu imu, %zu mocap messages",
            lidar_msgs_.size(), imu_msgs_.size(), mocap_msgs_.size());

        lidar_timer_ = create_wall_timer(
            std::chrono::milliseconds(50),
            std::bind(&LidarPicklerRepublisher::publish_lidar, this));

        imu_timer_ = create_wall_timer(
            std::chrono::milliseconds(10),
            std::bind(&LidarPicklerRepublisher::publish_imu, this));

        mocap_timer_ = create_wall_timer(
            std::chrono::milliseconds(20),
            std::bind(&LidarPicklerRepublisher::publish_mocap, this));
    }

private:
    // ---------------- LIDAR ----------------

    void load_lidar()
    {
        fs::path meta_path = fs::path(lidar_dir_) / "metadata.json";
        std::ifstream meta_file(meta_path);
        if (!meta_file.is_open())
            throw std::runtime_error("Failed to open lidar metadata.json");

        json meta;
        meta_file >> meta;

        auto files = list_files(lidar_dir_, "scan_", ".bin");
        RCLCPP_INFO(get_logger(), "Found %zu LiDAR scan files", files.size());

        for (const auto &f : files)
        {
            uint32_t sec, nsec;
            parse_timestamp_from_filename(f.filename().string(), sec, nsec);

            std::ifstream in(f, std::ios::binary);
            std::vector<uint8_t> data(
                (std::istreambuf_iterator<char>(in)),
                 std::istreambuf_iterator<char>());

            sensor_msgs::msg::PointCloud2 msg;
            msg.header.stamp.sec = sec;
            msg.header.stamp.nanosec = nsec;
            msg.header.frame_id = meta["frame_id"];

            msg.height = meta["height"];
            msg.width  = meta["width"];
            msg.is_bigendian = meta["is_bigendian"];
            msg.point_step = meta["point_step"];
            msg.row_step   = meta["row_step"];
            msg.is_dense   = meta["is_dense"];

            for (auto &f : meta["fields"])
            {
                sensor_msgs::msg::PointField pf;
                pf.name = f["name"];
                pf.offset = f["offset"];
                pf.datatype = f["datatype"];
                pf.count = f["count"];
                msg.fields.push_back(pf);
            }

            msg.data = std::move(data);
            lidar_msgs_.push_back(msg);
        }
    }

    void publish_lidar()
    {
        if (lidar_msgs_.empty()) return;
        if (lidar_idx_ >= lidar_msgs_.size())
        {
            if (!loop_) return;
            lidar_idx_ = 0;
        }
        lidar_pub_->publish(lidar_msgs_[lidar_idx_++]);
    }

    // ---------------- IMU ----------------

    void load_imu()
    {
        auto files = list_files(imu_dir_, "imu_", ".json");
        RCLCPP_INFO(get_logger(), "Found %zu IMU JSON files", files.size());

        for (const auto &f : files)
        {
            std::ifstream in(f);
            json j; in >> j;

            sensor_msgs::msg::Imu msg;
            msg.header.frame_id = j["frame_id"];
            msg.header.stamp.sec = j["timestamp_sec"];
            msg.header.stamp.nanosec = j["timestamp_nanosec"];

            msg.orientation.x = j["orientation"]["x"];
            msg.orientation.y = j["orientation"]["y"];
            msg.orientation.z = j["orientation"]["z"];
            msg.orientation.w = j["orientation"]["w"];

            for (int i = 0; i < 9; ++i)
                msg.orientation_covariance[i] = j["orientation_covariance"][i];

            msg.angular_velocity.x = j["angular_velocity"]["x"];
            msg.angular_velocity.y = j["angular_velocity"]["y"];
            msg.angular_velocity.z = j["angular_velocity"]["z"];

            for (int i = 0; i < 9; ++i)
                msg.angular_velocity_covariance[i] =
                    j["angular_velocity_covariance"][i];

            msg.linear_acceleration.x = j["linear_acceleration"]["x"];
            msg.linear_acceleration.y = j["linear_acceleration"]["y"];
            msg.linear_acceleration.z = j["linear_acceleration"]["z"];

            for (int i = 0; i < 9; ++i)
                msg.linear_acceleration_covariance[i] =
                    j["linear_acceleration_covariance"][i];

            imu_msgs_.push_back(msg);
        }
    }

    void publish_imu()
    {
        if (imu_msgs_.empty()) return;
        if (imu_idx_ >= imu_msgs_.size())
        {
            if (!loop_) return;
            imu_idx_ = 0;
        }
        imu_pub_->publish(imu_msgs_[imu_idx_++]);
    }

    // ---------------- MOCAP ----------------

    void load_mocap()
    {
        fs::path path = fs::path(csv_dir_) / "mocap_poses.csv";
        std::ifstream in(path);
        if (!in.is_open())
            throw std::runtime_error("Failed to open mocap_poses.csv");

        std::string line;
        std::getline(in, line); // header

        size_t count = 0;
        while (std::getline(in, line))
        {
            std::stringstream ss(line);
            std::string field;

            geometry_msgs::msg::PoseStamped msg;
            double ts;

            std::getline(ss, field, ','); ts = std::stod(field);

            msg.header.stamp.sec = static_cast<uint32_t>(ts);
            msg.header.stamp.nanosec =
                static_cast<uint32_t>((ts - msg.header.stamp.sec) * 1e9);

            msg.header.frame_id = "world";

            std::getline(ss, field, ','); msg.pose.position.x = std::stod(field);
            std::getline(ss, field, ','); msg.pose.position.y = std::stod(field);
            std::getline(ss, field, ','); msg.pose.position.z = std::stod(field);

            std::getline(ss, field, ','); msg.pose.orientation.x = std::stod(field);
            std::getline(ss, field, ','); msg.pose.orientation.y = std::stod(field);
            std::getline(ss, field, ','); msg.pose.orientation.z = std::stod(field);
            std::getline(ss, field, ','); msg.pose.orientation.w = std::stod(field);

            mocap_msgs_.push_back(msg);
            count++;
        }

        RCLCPP_INFO(get_logger(), "Found %zu mocap pose entries", count);
    }

    void publish_mocap()
    {
        if (mocap_msgs_.empty()) return;
        if (mocap_idx_ >= mocap_msgs_.size())
        {
            if (!loop_) return;
            mocap_idx_ = 0;
        }
        mocap_pub_->publish(mocap_msgs_[mocap_idx_++]);
    }

    // ---------------- Helpers ----------------

    static std::vector<fs::path> list_files(
        const std::string &dir,
        const std::string &prefix,
        const std::string &ext)
    {
        std::vector<fs::path> out;

        for (const auto &e : fs::directory_iterator(dir))
        {
            const std::string name = e.path().filename().string();

            if (name.size() < prefix.size() + ext.size())
                continue;

            const bool has_prefix =
                name.compare(0, prefix.size(), prefix) == 0;

            const bool has_suffix =
                name.compare(name.size() - ext.size(), ext.size(), ext) == 0;

            if (has_prefix && has_suffix)
                out.push_back(e.path());
        }

        std::sort(out.begin(), out.end());
        return out;
    }

    static void parse_timestamp_from_filename(
        const std::string &name,
        uint32_t &sec,
        uint32_t &nsec)
    {
        auto a = name.find('_');
        auto b = name.find('_', a + 1);
        auto c = name.find('.', b);

        sec  = std::stoul(name.substr(a + 1, b - a - 1));
        nsec = std::stoul(name.substr(b + 1, c - b - 1));
    }

    // ---------------- Members ----------------

    std::string lidar_dir_, imu_dir_, csv_dir_;
    bool loop_{true};

    rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr lidar_pub_;
    rclcpp::Publisher<sensor_msgs::msg::Imu>::SharedPtr imu_pub_;
    rclcpp::Publisher<geometry_msgs::msg::PoseStamped>::SharedPtr mocap_pub_;

    rclcpp::TimerBase::SharedPtr lidar_timer_;
    rclcpp::TimerBase::SharedPtr imu_timer_;
    rclcpp::TimerBase::SharedPtr mocap_timer_;

    std::vector<sensor_msgs::msg::PointCloud2> lidar_msgs_;
    std::vector<sensor_msgs::msg::Imu> imu_msgs_;
    std::vector<geometry_msgs::msg::PoseStamped> mocap_msgs_;

    size_t lidar_idx_{0};
    size_t imu_idx_{0};
    size_t mocap_idx_{0};
};

int main(int argc, char **argv)
{
    rclcpp::init(argc, argv);
    rclcpp::spin(std::make_shared<LidarPicklerRepublisher>());
    rclcpp::shutdown();
    return 0;
}
