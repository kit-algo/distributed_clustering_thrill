#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>

#include <ostream>
#include <random>

struct Point {
  double x, y;
};

std::ostream& operator << (std::ostream& os, const Point& point) {
  return os << '(' << point.x << ',' << point.y << ')';
}

void Process(thrill::Context& context) {
  std::default_random_engine rng(std::random_device {} ());
  std::uniform_real_distribution<double> dist(0.0, 1000.0);

  auto points = thrill::Generate(context, 100, [&](const size_t&) {
    return Point { dist(rng), dist(rng) };
  }).Cache();

  points.Print("Points");
}

int main() {
  return thrill::Run([&](thrill::Context& context) { Process(context); });
}

