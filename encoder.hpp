#ifndef KANTAPLUS_ENCODER_HPP
#define KANTAPLUS_ENCODER_HPP

#include <cstdint>
#include <vector>
#include <string>

namespace Encoder {
	std::vector<char> encode_entry(std::uint32_t ts, std::uint32_t ksize,
															 std::uint32_t vsize, const std::string &key,
															 const std::string &value);

	std::string decode_entry(std::vector<char> buffer);
}

#endif
