#include "gtest/gtest.h"
#include "../src/DBImpl.h"

TEST(PrimeTest, IsThree) {
ASSERT_TRUE(isPrime(3));
}

TEST(PrimeTest, IsTen) {
ASSERT_FALSE(isPrime(10));
}
