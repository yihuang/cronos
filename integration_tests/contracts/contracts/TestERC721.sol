pragma solidity ^0.6.6;

import "@openzeppelin/contracts/token/ERC721/ERC721.sol";

contract ERC721Token is ERC721("NFT Token", "NFT") {
    uint256 public nextTokenId;

    function mint() public {
        _mint(msg.sender, nextTokenId);
        nextTokenId++;
    }
}
