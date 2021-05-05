#include "encoder.hpp"
#include <cstdint>

std::vector<char> Encoder::encode_entry(std::uint32_t ts, std::uint32_t ksize,
														std::uint32_t vsize, const std::string &key,
														const std::string &value) {
	std::vector<char> buffer;

	auto ts_buffer = convert_uint32_to_bytes(ts);
	buffer.insert(buffer.end(), ts_buffer.begin(), ts_buffer.end());

	auto key_buffer = convert_uint32_to_bytes(ts);
	buffer.insert(buffer.end(), key_buffer.begin(), key_buffer.end());

	auto val_buffer = convert_uint32_to_bytes(ts);
	buffer.insert(buffer.end(), val_buffer.begin(), val_buffer.end());

	buffer.insert(buffer.end(), key.begin(), key.end());
	buffer.insert(buffer.end(), value.begin(), value.end());

	return buffer;
}

std::vector<char> Encoder::convert_uint32_to_bytes(std::uint32_t to_convert) {
	std::vector<char> buf(4, 0);
	std::uint32_t uval = to_convert;
	buf[0] = uval;
	buf[1] = uval >> 8;
	buf[2] = uval >> 16;
	buf[3] = uval >> 24;

	return buf;
}
